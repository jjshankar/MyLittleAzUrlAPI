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
        // private int _nxtId;

        public LittleUrlAzureContext()
        {
            string storageConnectionString;

            // Read from config file
            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");   

            storageConnectionString = configBuilder.Build().GetValue<string>("AzureTable:ConnectionString");
            _tableName = configBuilder.Build().GetValue<string>("AzureTable:TableName");
            _partitionKey = configBuilder.Build().GetValue<string>("AzureTable:PartitionString");

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

        public async Task<TableResult> GetUrl(string sKey)
        {
            // Setup Table op
            TableOperation retrieveOp = TableOperation.Retrieve<LittleUrlAzure>(_partitionKey, sKey);

            bool bExists = await _littleUrlTable.ExistsAsync();
            if (bExists)
            {
                TableResult result = await _littleUrlTable.ExecuteAsync(retrieveOp);
                return result;
            }
            return null;
        }

        public async Task<List<LittleUrlAzure>> ListUrl()
        {
            List<LittleUrlAzure> listToReturn = new List<LittleUrlAzure>();
            //return listToReturn;

            // Construct the query operation for all entities in PartitionKey.
            TableQuery<LittleUrlAzure> query = new TableQuery<LittleUrlAzure>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, _partitionKey));

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
                // Construct the query operation for all customer entities where PartitionKey="Smith".
                TableQuery<LittleUrlAzure> query = new TableQuery<LittleUrlAzure>()
                    .Where(TableQuery.GenerateFilterCondition("LongUrl", QueryComparisons.Equal, longUrl));

                // Collect the urls in the list
                TableContinuationToken token = null;
                TableQuerySegment<LittleUrlAzure> resultSegment =
                    await _littleUrlTable.ExecuteQuerySegmentedAsync<LittleUrlAzure>(query, token);

                itemRet = resultSegment.Results.FirstOrDefault<LittleUrlAzure>();
            }
            catch (Exception ex)
            {
                Debug.WriteLine("CheckDupe() | Exception caught:" + ex.Message);
                itemRet = null;
            }

            return itemRet;
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
