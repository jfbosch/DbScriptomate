﻿using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace NextSequenceNumber.Contracts
{
	public static class TableStorageNumberStore
	{
		private static readonly object _locker = new object();

		public static string GetNextSequenceNumber(string key)
		{
			lock (_locker)
			{
				return ReadAndIncrementSequenceNumber(key);
			}
		}

		private static string ReadAndIncrementSequenceNumber(string key)
		{
			CloudStorageAccount storageAccount =
				CloudStorageAccount.Parse(System.Configuration.ConfigurationManager.AppSettings.Get("AzureStorageAddress"));


			var tableClient = storageAccount.CreateCloudTableClient();
			var tableRef = tableClient.GetTableReference(System.Configuration.ConfigurationManager.AppSettings.Get("AzureTableName"));
			tableRef.CreateIfNotExists();

			var query = tableRef.CreateQuery<SequenceEntity>();
			var lastEntity = query.Where(o => o.PartitionKey == key).FirstOrDefault();

			var concurrencyTag = lastEntity?.ETag;

			var lastNumber = "00000";
			if (lastEntity != null)
			{
				lastNumber = lastEntity.Number;
			}
			else
			{
				lastEntity = new SequenceEntity() { PartitionKey = key, RowKey = "1" };
			}

			int number;
			if (!int.TryParse(lastNumber, out number))
				return "failed to parse: " + lastNumber;

			number++;
			string nextNumber = number.ToString("00000");

			// save back to table
			lastEntity.Number = nextNumber;
			lastEntity.ETag = concurrencyTag;
			tableRef.Execute(TableOperation.InsertOrReplace(lastEntity));

			return nextNumber;
		}

		public class SequenceEntity : TableEntity
		{
			public string Number { get; set; }
		}
	}
}
