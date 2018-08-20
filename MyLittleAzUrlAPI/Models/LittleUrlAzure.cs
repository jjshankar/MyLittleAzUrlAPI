using System;
using Microsoft.WindowsAzure.Storage.Table;


namespace MyLittleAzUrlAPI.Models
{
    public class LittleUrlAzure : TableEntity
    {
        public int UrlId
        {
            get;
            set;
        }

        public string ShortUrl
        {
            get => this.RowKey;
        }

        public string LongUrl
        {
            get;
            set;
        }

        public DateTime CreationTime
        {
            get => this.Timestamp.DateTime;
        }

        public LittleUrlAzure()
        {
        }

        public LittleUrlAzure(string partition, string key, int id, string value)
        {
            this.PartitionKey = partition;
            this.RowKey = key;
            this.LongUrl = value;
            this.UrlId = id;
        }
    }
}
