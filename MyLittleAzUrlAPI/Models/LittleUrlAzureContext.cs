using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;


namespace MyLittleAzUrlAPI.Models
{
    public class LittleUrlAzureContext
    {
        private CloudStorageAccount _storageAccount;
        private CloudTableClient _tableClient;
        private CloudTable _littleUrlTable;
        private string _tableName;
        private string _partitionKey;

        // retention/undelete 
        private int _retentionDays;
        private DateTime _lastPurgeDate;
        private DateTime _lastRetentionDate;
        // private int _nxtId;

        public LittleUrlAzureContext()
        {
            string storageConnectionString;

            // Read from config file
            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables();

            storageConnectionString = configBuilder.Build().GetValue<string>("AzureTable:ConnectionString");
            _tableName = configBuilder.Build().GetValue<string>("AzureTable:TableName");

            // Implementing retention/undelete
            // Read purge window from config; if not valid, use 90 as default
            string purgeAfter = configBuilder.Build().GetValue<string>("AzureTable:RetentionDays");
            _retentionDays = int.TryParse(purgeAfter, out _retentionDays) ? _retentionDays : 90;
            _lastPurgeDate = DateTime.MinValue;
            _lastRetentionDate = DateTime.MinValue;

            //  Using the state as the partition key - default: True
            // _partitionKey = configBuilder.Build().GetValue<string>("AzureTable:PartitionString");
            _partitionKey = bool.TrueString;

            // Connect
            _storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Open Azure Table
            _CreateOrOpenTable().Wait();
        }

        private async Task<CloudTable> _CreateOrOpenTable()
        {
            try
            {
                // Create table if it does not exist
                _tableClient = _storageAccount.CreateCloudTableClient();

                _littleUrlTable = _tableClient.GetTableReference(_tableName);

                if (await _littleUrlTable.CreateIfNotExistsAsync())
                    Debug.WriteLine(String.Format("Table {0} created.", _tableName));
                else
                    Debug.WriteLine(String.Format("Table {0} exits.", _tableName));
            }
            catch (Exception ex)
            {
                Debug.WriteLine("_CreateOrOpenTable() | Exception caught:" + ex.Message);
            }

            return _littleUrlTable;
        }

        public async Task<TableResult> InsertUrl(int urlId, string urlToInsert)
        {
            TableResult result = null;

            try
            {
                string sKey = GetNewKey();
                LittleUrlAzure littleUrl = new LittleUrlAzure(_partitionKey, sKey, urlId, urlToInsert);

                // Create the insert op (use upsert to prevent key duplication)
                TableOperation insertOp = TableOperation.InsertOrMerge(littleUrl);

                // Execute
                result = await _littleUrlTable.ExecuteAsync(insertOp);
            }
            catch (Exception ex)
            {
                Debug.WriteLine("InsertUrl() | Exception caught:" + ex.Message);
                result = null;
            }
            return result;
        }

        public async Task<TableResult> GetUrl(string sKey, bool bIsUrlDeleted = false)
        {
            // Call purge operation whenever a Get is called
            //  The op runs only once a day
            if (_lastPurgeDate.AddDays(1) < DateTime.UtcNow)
                PurgeUrls();

            if (!string.IsNullOrEmpty(sKey))
            {

                // Setup Table op to retrieve from the appropriate partition
                string partitionKey = (!bIsUrlDeleted).ToString();
                TableOperation retrieveOp = TableOperation.Retrieve<LittleUrlAzure>(partitionKey, sKey);

                bool bExists = await _littleUrlTable.ExistsAsync();
                if (bExists)
                {
                    TableResult result = await _littleUrlTable.ExecuteAsync(retrieveOp);

                    // If found, update last accessed time
                    LittleUrlAzure item = (LittleUrlAzure)result.Result;
                    if(item != null)
                    {
                        item.LastAccessedTime = DateTime.UtcNow;
                        TableOperation updateOp = TableOperation.InsertOrReplace(item);
                        await _littleUrlTable.ExecuteAsync(updateOp);
                    }

                    // Perform retention management once a day
                    if (_lastRetentionDate.AddDays(1) < DateTime.UtcNow)
                        RemoveStaleUrls();

                    return result;
                }
            }
            return null;
        }

        public async Task<List<LittleUrlAzure>> ListUrl()
        {
            List<LittleUrlAzure> listToReturn = new List<LittleUrlAzure>();
            //return listToReturn;

            // Construct the query operation for all entities (querying both partitions).
            TableQuery<LittleUrlAzure> query = new TableQuery<LittleUrlAzure>()
                .Where(TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, bool.TrueString),
                        TableOperators.Or,
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, bool.FalseString)));
            
            // Collect the urls in the list
            TableContinuationToken token = null;
            do
            {
                TableQuerySegment<LittleUrlAzure> resultSegment =
                    await _littleUrlTable.ExecuteQuerySegmentedAsync<LittleUrlAzure>(query, token);

                token = resultSegment.ContinuationToken;

                foreach (LittleUrlAzure entity in resultSegment.Results)
                {
                    listToReturn.Add(entity);
                }
            } while (token != null);

            return listToReturn;
        }


        public async Task<LittleUrlAzure> CheckDupe(string longUrl)
        {
            LittleUrlAzure itemRet = null;

            try
            {
                // Construct the query operation
                TableQuery<LittleUrlAzure> query = new TableQuery<LittleUrlAzure>()
                    .Where(TableQuery.GenerateFilterCondition("LongUrl", QueryComparisons.Equal, longUrl));

                // Collect the urls in the list
                TableContinuationToken token = null;
                TableQuerySegment<LittleUrlAzure> resultSegment =
                    await _littleUrlTable.ExecuteQuerySegmentedAsync<LittleUrlAzure>(query, token);

                itemRet = resultSegment.Results.FirstOrDefault<LittleUrlAzure>();

                // If this URL is deleted; undelete and return
                if(itemRet.IsDeleted)
                    itemRet = await ToggleDelete(itemRet.ShortUrl, false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine("CheckDupe() | Exception caught:" + ex.Message);
                itemRet = null;
            }

            return itemRet;
        }

        // Retention/Undelete
        public async Task<LittleUrlAzure> ToggleDelete(string shortUrl, bool bDeleteThis)
        { 
            // Move url to 'True' or 'False' partitions based on current state
            if (!string.IsNullOrEmpty(shortUrl))
            {
                // Setup Table op from appropriate partition 
                TableOperation retrieveOp = TableOperation.Retrieve<LittleUrlAzure>((bDeleteThis).ToString(), shortUrl);
                TableResult result = await _littleUrlTable.ExecuteAsync(retrieveOp);
                LittleUrlAzure item = (LittleUrlAzure)result.Result;

                if(item!= null)
                {
                    // Found
                    //  Now delete it from this partition and recreate in the other
                    TableOperation deleteOp = TableOperation.Delete(item);
                    await _littleUrlTable.ExecuteAsync(deleteOp);

                    // If we are deleting this entity
                    if (bDeleteThis)
                    {
                        // Set deleted time
                        item.DeletedTime = DateTime.UtcNow;

                        // Set retention limit (midnight 90 days from now)
                        DateTime purgeDate = DateTime.UtcNow.AddDays(_retentionDays);
                        item.PurgeDate = new DateTime(purgeDate.Year, purgeDate.Month, purgeDate.Day,
                                                        23, 59, 59, 999, DateTimeKind.Utc);
                    }
                    else
                    {
                        // We are undeleting; reset retention dates
                        item.DeletedTime = DateTime.MinValue;
                        item.PurgeDate = DateTime.MinValue;
                    }

                    // Insert into the other partition
                    item.PartitionKey = (!bDeleteThis).ToString();
                    TableOperation insertOp = TableOperation.Insert(item);
                    await _littleUrlTable.ExecuteAsync(insertOp);

                    // Return
                    return item;
                }
            }
            return null;
        }

        private async void PurgeUrls()
        {
            // Permanenty delete urls from the 'False' partition that have expired today
            try
            {
                // Construct the query operation
                TableQuery<LittleUrlAzure> query = new TableQuery<LittleUrlAzure>()
                    .Where(TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, bool.FalseString),
                        TableOperators.And,
                        TableQuery.GenerateFilterConditionForDate("PurgeDate", QueryComparisons.LessThanOrEqual, _lastPurgeDate)));

                // Collect the urls in the list
                TableContinuationToken token = null;
                do
                {
                    TableQuerySegment<LittleUrlAzure> resultSegment =
                        await _littleUrlTable.ExecuteQuerySegmentedAsync<LittleUrlAzure>(query, token);
                    token = resultSegment.ContinuationToken;

                    foreach (LittleUrlAzure entity in resultSegment.Results)
                    {
                        // Delete
                        TableOperation deleteOp = TableOperation.Delete(entity);
                        await _littleUrlTable.ExecuteAsync(deleteOp);

                    }
                } while (token != null);
            }
            catch(Exception ex)
            {
                Debug.WriteLine("PurgeUrls() | Exception caught:" + ex.Message);
            }

            return;
        }

        public async void RemoveStaleUrls()
        { 
            // Logically delete urls not accessed for x days (x = 90)
            //  Move expired urls from the 'True' partition to the 'False' partition
            try
            {
                // Construct the query operation
                TableQuery<LittleUrlAzure> query = new TableQuery<LittleUrlAzure>()
                    .Where(TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, bool.TrueString),
                        TableOperators.And,
                        TableQuery.GenerateFilterConditionForDate("LastAccessedTime", QueryComparisons.GreaterThan,
                                                                  DateTime.UtcNow.AddDays(_retentionDays))));

                // Collect the urls in the list
                TableContinuationToken token = null;
                do
                {
                    TableQuerySegment<LittleUrlAzure> resultSegment =
                        await _littleUrlTable.ExecuteQuerySegmentedAsync<LittleUrlAzure>(query, token);
                    token = resultSegment.ContinuationToken;

                    foreach (LittleUrlAzure entity in resultSegment.Results)
                    {
                        // Delete
                        TableOperation deleteOp = TableOperation.Delete(entity);
                        await _littleUrlTable.ExecuteAsync(deleteOp);

                        // Insert into the deleted partition
                        entity.PartitionKey = bool.FalseString;
                        entity.DeletedTime = DateTime.UtcNow;

                        // Set retention limit (midnight 90 days from now)
                        DateTime purgeDate = DateTime.UtcNow.AddDays(_retentionDays);
                        entity.PurgeDate = new DateTime(purgeDate.Year, purgeDate.Month, purgeDate.Day,
                                                        23, 59, 59, 999, DateTimeKind.Utc); 
                        
                        TableOperation insertOp = TableOperation.Insert(entity);
                        await _littleUrlTable.ExecuteAsync(insertOp);
                    }
                } while (token != null);
            }
            catch(Exception ex)
            {
                Debug.WriteLine("RemoveStaleUrls() | Exception caught:" + ex.Message);
            }
        }

        // Private helper
        private string GetNewKey()
        {
            string sNewKey;
            TableResult item = null;

            byte[] b = new byte[3];
            Random rnd = new Random();
            Regex rx = new Regex(@"([A-Za-z0-9]){3}");

            do
            {
                do
                {
                    rnd.NextBytes(b);
                    sNewKey = Convert.ToBase64String(b).Substring(0, 3);
                }
                while (!rx.IsMatch(sNewKey));
            }
            while (item != null);

            return sNewKey.ToLower();
        }

    }
}
