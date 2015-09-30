using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Web;
using System.Web.Http;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace NextSequenceNumber.Service
{
	internal static class NumberStore
	{
		private static readonly object _locker = new object();

		internal static string GetNextSequenceNumber(string key)
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

			var lastNumber = "00000";
			if (lastEntity != null)
			{
				lastNumber = lastEntity.Number;
			}
			else
			{
				lastEntity = new SequenceEntity(){PartitionKey=key,RowKey="1"};
			}

			int number;
			if (!int.TryParse(lastNumber, out number))
				return "failed to parse: " + lastNumber;

			number++;
			string nextNumber = number.ToString("00000");
			
			// save back to table
			lastEntity.Number = nextNumber;
			tableRef.Execute(TableOperation.InsertOrReplace(lastEntity));

			return nextNumber;
		}

		public class SequenceEntity : TableEntity
		{
			public string Number { get; set; }
		}
	}
}
