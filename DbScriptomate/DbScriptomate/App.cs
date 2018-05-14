using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using NextSequenceNumber.Contracts;
using ServiceStack.ServiceClient.Web;
using ServiceStack.Text;

namespace DbScriptomate
{
	internal class App
	{
		private readonly Regex _sequenceRegex = new Regex(@"(^\d{3,}(\.\d+){0,1})[.,].*");
		private RunArguments _runArgs;
		private DirectoryInfo _appDir;

		internal App(
			RunArguments runArgs,
			DirectoryInfo appDir)
		{
			_runArgs = runArgs;
			_appDir = appDir;
		}

		private void InitialSetup()
		{
			ConnectionStringSettings conSettings = LetUserPickDbConnection("");

			var dbInfrastructure = new DirectoryInfo(Path.Combine(_appDir.FullName, @"_DbInfrastructure\DbObjects"));
			var scripts = dbInfrastructure.GetFiles("*.sql", SearchOption.AllDirectories).AsEnumerable()
				 .OrderBy(f => f.Name)
				 .ToList();

			scripts.ForEach(s =>
			{
				string result;
				RunDbScript(conSettings, s, out result);
				result = string.IsNullOrWhiteSpace(result) ? "Success" : result;
				Console.WriteLine("Ran {0} with result: {1}", s.Name, result);
			});

			var scriptTemplatesDirectory = Path.Combine(_appDir.FullName, @"_DbInfrastructure\ScriptTemplates");
			var scriptsTemplates = Directory.GetFiles(scriptTemplatesDirectory, "*.sql");
			var templateDirectoryName = conSettings.Name.Replace('\\', '-');
			var templateDirectory = Path.Combine(_appDir.FullName, templateDirectoryName);

			Directory.CreateDirectory(templateDirectory);

			foreach (var template in scriptsTemplates)
			{
				var filename = Path.Combine(templateDirectory, Path.GetFileName(template));
				File.Copy(template, filename, overwrite: true);
			}
		}

		private List<FileInfo> GetExistingScriptFiles(DirectoryInfo dbDir)
		{
			var scripts = dbDir.GetFiles("*.sql", SearchOption.AllDirectories).AsEnumerable()
				.Where(f => !f.Directory.Name.StartsWith("_"))
				.Where(f => _sequenceRegex.IsMatch(f.Name))
				.OrderBy(f => ToDecimal(f.Name))
				.ToList();
			return scripts;
		}

		private DirectoryInfo LetUserPickDbDir(DirectoryInfo currentDir)
		{
			var dbDirs = currentDir.GetDirectories()
				.Where(d => !d.Name.StartsWith("_"))
				.OrderBy(d => d.Name)
				.ToArray();
			if (!dbDirs.Any())
			{
				Console.WriteLine("No DB Folders found.");
				Environment.Exit(0);
			}
			for (int i = 1; i <= dbDirs.Count(); i++)
			{
				var info = string.Format("{0}) {1}", i, dbDirs[i - 1].Name);
				Console.WriteLine(info);
			}

			var userOption = Console.ReadKey();
			Console.Clear();
			int selectedIndex = int.Parse(userOption.KeyChar.ToString()) - 1;
			if (selectedIndex >= dbDirs.Count())
			{
				Console.WriteLine("Invalid selection.");
				Environment.Exit(0);
			}
			var dbDir = dbDirs[selectedIndex];
			return dbDir;
		}

		private void ApplyScriptsToDb(
			RunArguments runArgs,
			DirectoryInfo dbDir)
		{
			var scripts = GetExistingScriptFiles(dbDir);
			if (scripts.Count() == 0)
				Console.WriteLine("No previous scripts detected.");

			ConnectionStringSettings conSettings;
			if (runArgs.RunMode == RunMode.Interactive)
				conSettings = LetUserPickDbConnection(dbDir.Name);
			else if (runArgs.RunMode == RunMode.ApplyScriptsToDb)
				conSettings = new ConnectionStringSettings(runArgs.DbKey, runArgs.DbConnectionString, runArgs.DbConnectionProvider);
			else
				throw new InvalidArgumentException("Invalid execution path.");

			ApplyMissingScripts(runArgs, conSettings, scripts);
		}

		private ConnectionStringSettings LetUserPickDbConnection(
			string connectionPrefix)
		{
			Console.WriteLine("Pick connection string to use for:" + connectionPrefix);
			// If a connection string contains \ we replace it with -
			// We will use "Like" here so we remove the - delimiter and replace with % that the Regex will use
			// to decide whether there is a match
			connectionPrefix = connectionPrefix.Replace('-', '%') + "%";
			var connectionList = ConfigurationManager.ConnectionStrings.Cast<ConnectionStringSettings>()
				.Where(cs => cs.Name != "LocalSqlServer")
					 .Where(cs => cs.Name.Like(connectionPrefix))
				.ToList();
			foreach (ConnectionStringSettings cs in connectionList)
			{
				var index = 1 + connectionList.IndexOf(cs);
				var connectionInfo = string.Format("{0}) {1}", index, cs.Name);
				Console.WriteLine(connectionInfo);
			}
			var input = Console.ReadKey();
			Console.Clear();
			int selectedIndex = int.Parse(input.KeyChar.ToString()) - 1;
			Console.WriteLine("{1} selected: {0}", connectionList[selectedIndex].Name, selectedIndex + 1);
			var con = connectionList[selectedIndex];
			return con;
		}

		private void ApplyMissingScripts(
			RunArguments runArgs,
			ConnectionStringSettings conSettings,
			IList<FileInfo> scripts)
		{
			var scriptsToRun = new List<FileInfo>();
			var dbScriptNumbers = GetDbScripts(conSettings);
			Console.WriteLine("{0} scripts logged in dbo.DbScripts", dbScriptNumbers.Count());
			Console.WriteLine("The following are scripts not yet run on the selected DB:");
			foreach (var scriptFile in scripts)
			{
				var scriptNumber = ToDecimal(scriptFile.Name);
				if (!dbScriptNumbers.Contains(scriptNumber))
				{
					PrintScriptItemInfo(scriptFile);
					scriptsToRun.Add(scriptFile);
				}
			}
			Console.WriteLine("Missing scripts check completed");
			bool userSelectedToApplyScripts = false;
			if (runArgs.RunMode == RunMode.Interactive)
			{
				Console.WriteLine("Would you like me to run them one at a time? I'll break on any errors?");
				Console.WriteLine("1 - Yes, please run one at a time.");
				Console.WriteLine("2 - No thanks, I'll run them  later myself.");
				int selectedIndex = int.Parse(Console.ReadKey().KeyChar.ToString());
				Console.Clear();

				if (selectedIndex == 1)
					userSelectedToApplyScripts = true;
			}

			if (runArgs.RunMode == RunMode.Interactive && userSelectedToApplyScripts
				|| runArgs.RunMode == RunMode.ApplyScriptsToDb)
			{
				foreach (var scriptFile in scriptsToRun)
				{
					Console.WriteLine("Running {0}", scriptFile.Name);
					string errorMessage = string.Empty;
					bool success = RunDbScript(conSettings, scriptFile, out errorMessage);
					Console.WriteLine(success ? "Succeeded" : "Failed:");
					if (!success)
					{
						Console.WriteLine("{0}", errorMessage);
						if (runArgs.RunMode == RunMode.Interactive)
						{
							Console.WriteLine("1 - Skip, 2 - Abort?");
							var innerInput = Console.ReadKey();
							int selectedIndex = int.Parse(innerInput.KeyChar.ToString());
							if (selectedIndex == 2)
								return;
						}
						// Not interactive
						Environment.Exit(2222);
					}
				}
			}
		}

		private void PrintScriptItemInfo(FileInfo scriptFile)
		{
			var item = string.Format("{0}", scriptFile.Name);
			if (item.Length > 75)
			{
				item = item.Substring(0, Math.Min(item.Length, 75));
				item += "...";
			}
			Console.WriteLine(item);
		}

		private bool RunDbScript(ConnectionStringSettings connectionSettings, FileInfo scriptFile, out string value)
		{
			value = "";
			string sql = scriptFile.OpenText().ReadToEnd();
			var builder = new SqlConnectionStringBuilder(connectionSettings.ConnectionString);
			builder.MultipleActiveResultSets = false;
			var connectionString = builder.ToString();
			var sqlConnection = new SqlConnection(connectionString);
			Server server = null;
			try
			{
				var serverConnection = new ServerConnection(sqlConnection);
				server = new Server(serverConnection);
				server.ConnectionContext.BeginTransaction();
				server.ConnectionContext.ExecuteNonQuery(sql);
				server.ConnectionContext.CommitTransaction();
			}
			catch (Exception e)
			{
				if (server != null)
					server.ConnectionContext.RollBackTransaction();

				e = e.GetBaseException();
				value = e.Message;
				return false;
			}
			finally
			{
				if (sqlConnection != null)
					sqlConnection.Dispose();
			}
			return true;
		}

		private List<decimal> GetDbScripts(ConnectionStringSettings connectionSettings)
		{
			var dbScriptNumbers = new List<decimal>();
			using (SqlConnection connection = new SqlConnection(connectionSettings.ConnectionString))
			{
				var sql = @"select ScriptNumber from dbo.DbScripts";
				var command = new SqlCommand(sql, connection);
				command.CommandType = System.Data.CommandType.Text;
				connection.Open();
				var reader = command.ExecuteReader();
				while (reader.Read())
				{
					var value = reader["ScriptNumber"].ToString();
					dbScriptNumbers.Add(decimal.Parse(value));
				}
				reader.Close();
				connection.Close();
				return dbScriptNumbers;
			}
		}

		private void RunInteractively(
			RunArguments runArgs,
			DirectoryInfo dbDir)
		{
			Console.WriteLine("Pick:");
			Console.WriteLine("1) Detect last script number and generate new script template");
			Console.WriteLine("2) Detect missing scripts in DB");
			Console.WriteLine("3) Setup your Database for DbScriptomate");
			Console.WriteLine("4) Generate Db Objects");
			var input = Console.ReadKey();

			Console.Clear();
			switch (input.KeyChar)
			{
				case '1':
					GenerateNewScript(runArgs, dbDir);
					break;
				case '2':
					ApplyScriptsToDb(runArgs, dbDir);
					break;
				case '3':
					InitialSetup();
					break;
				case '4':
					GenerateDbObjects(runArgs,dbDir);
					break;
			}
		}

		private GetNextNumberResponse GetNextSequenceNumber(string key, bool useLocal = false)
		{
			bool goDirectToTableStorage = false;
			goDirectToTableStorage = bool.TryParse(ConfigurationManager.AppSettings["GoDirectToTableStorage"], out goDirectToTableStorage);

			if (goDirectToTableStorage)
			{
				return GetFromTableStorageDirectly(key);
			}
			else // either local based on date time, or remote web api service
			{
				if (useLocal)
				{
					return new GetNextNumberResponse
					{
						ForKey = key,
						NextSequenceNumber = DateTime.UtcNow.ToString("yyMMddHHmmss")
					};
				}
				else
				{
					return GetFRomRemoteService(key);
				}
			}
		}

		private GetNextNumberResponse GetFromTableStorageDirectly(string key)
		{
			var response = new GetNextNumberResponse
			{
				ForKey = key,
			};

			response.NextSequenceNumber = TableStorageNumberStore.GetNextSequenceNumber(key);
			return response;
		}

		private static GetNextNumberResponse GetFRomRemoteService(string key)
		{
			string url = (string)new AppSettingsReader().GetValue("NextSequenceNumberServiceUrl", typeof(string));
			if (url.Contains("api"))
			{
				using (var client = new HttpClient())
				{
					var password = System.Configuration.ConfigurationManager.AppSettings.Get("Password");
					var webapiUrl = string.Format("{0}?key={1}&password={2}", url, key, password);
					var next = client.GetStringAsync(webapiUrl).Result;
					var serializer = new JsonSerializer<string>();
					var value = serializer.DeserializeFromString(next);
					return new GetNextNumberResponse() { NextSequenceNumber = value, ForKey = key };
				}
			}

			using (var client = new JsonServiceClient(url))
			{
				var response = client.Post<GetNextNumberResponse>(new GetNextNumber { ForKey = key });
				return response;
			}
		}

		private void GenerateNewScript(
			RunArguments runArgs,
			DirectoryInfo dbDir)
		{
			var response = GetNextSequenceNumber(dbDir.Name, runArgs.UseLocal);
			Console.WriteLine(response.ToString());
			runArgs.ScriptNumber = response.NextSequenceNumber;
			var newScript = CreateNewScriptFile(dbDir, runArgs);

			Console.WriteLine("using {0} next", runArgs.ScriptNumber);
			Console.WriteLine("created file: {0}\\{1}", dbDir.Name, newScript);
			Console.WriteLine();

			var shouldDenerateDbObjects = false;
			bool.TryParse(System.Configuration.ConfigurationManager.AppSettings.Get("GenerateDbObjectsOnNewScript"), out shouldDenerateDbObjects);

			if (shouldDenerateDbObjects)
			{
				Console.WriteLine("Automatically generating db object scripts (GenerateDbObjectsOnNewScript)");
				Console.WriteLine("Your script file is available for you to carry on with while this process is busy");
				GenerateDbObjects(runArgs, dbDir);
			}

		}

		private decimal ToDecimal(string filename)
		{
			var value = _sequenceRegex.Match(filename).Groups[1].Value;
			var num = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
			return num;
		}

		private string CreateNewScriptFile(
			DirectoryInfo dir,
			RunArguments attributes)
		{
			const string fileNameFormat = "{0:000.0}.{1}.{2}.{3}.sql";
			string templateFile = "_NewScriptTemplate.sql";
			templateFile = Path.Combine(dir.FullName, templateFile);
			if (!File.Exists(templateFile))
				throw new FileNotFoundException(templateFile);
			var template = File.ReadAllText(templateFile);
			var contents = string.Format(template, attributes.ScriptNumber, attributes.DdlOrDmlType, attributes.Author, attributes.Description);
			var filename = string.Format(fileNameFormat, attributes.ScriptNumber, attributes.Author, attributes.DdlOrDmlType, attributes.Description);
			File.AppendAllText(Path.Combine(dir.FullName, filename), contents, Encoding.UTF8);
			return filename;
		}

		internal void Execute()
		{
			switch (_runArgs.RunMode)
			{
				case RunMode.Interactive:
					{
						var dbDir = LetUserPickDbDir(_appDir);
						Console.WriteLine("Selected " + dbDir.Name);
						RunInteractively(_runArgs, dbDir);
						break;
					}
				case RunMode.GenerateNewScript:
					throw new NotImplementedException("DbKey needs to be implemented on this command line option");
					//GenerateNewScript(runArgs, dbDir);
					break;
				case RunMode.ApplyScriptsToDb:
					{
						var dbDir = new DirectoryInfo(_runArgs.DbDir);
						if (!dbDir.Exists)
						{
							Console.WriteLine("The following DB Dir does not exist: " + dbDir.FullName);
							Environment.Exit(1);
						}
						ApplyScriptsToDb(_runArgs, dbDir);
						break;
					}
				case RunMode.SetupDb:
					{
						InitialSetup();
						break;
					}
			}
		}

		private void GenerateDbObjects(RunArguments runArgs, DirectoryInfo dbDir)
		{
			var startTime = DateTime.Now;
			var dbConnection = LetUserPickDbConnection(dbDir.Name);
			var dbConnectionString = dbConnection.ConnectionString;
			Console.WriteLine("Generating Db Object files into .\\DbObjects");

			DirectoryInfo rootDir = dbDir;
			var targetDir = rootDir.CreateSubdirectory("DbObjects");
			rootDir.CreateSubdirectory("DbObjects\\Tables");
			rootDir.CreateSubdirectory("DbObjects\\Views");
			rootDir.CreateSubdirectory("DbObjects\\Programmability\\UserDefinedTableTypes");
			rootDir.CreateSubdirectory("DbObjects\\Programmability\\UserDefinedTypes");
			rootDir.CreateSubdirectory("DbObjects\\Programmability\\UserDefinedDataTypes");
			rootDir.CreateSubdirectory("DbObjects\\Programmability\\UserDefinedFunctions");
			rootDir.CreateSubdirectory("DbObjects\\Programmability\\StoredProcedures");

			var ignoreValue = System.Configuration.ConfigurationManager.AppSettings.Get("GenerateDbObjectsIgnoreSchemas");
			if (string.IsNullOrWhiteSpace(ignoreValue)) ignoreValue = string.Empty;

			var ignoreSchemas = ignoreValue
				.Split(new[] {" ", ",", ";"}, StringSplitOptions.RemoveEmptyEntries)
				.ToList();

			
			var allTasks = new List<Task>();
			allTasks.Add(Task.Run(() =>
			{
				var sqlConnection = new SqlConnection(dbConnectionString);
				var connection = new ServerConnection(sqlConnection);
				var server = new Server(connection);
				var db = server.Databases[sqlConnection.Database];
				var spScripter = new Scripter(server);
				spScripter.Options.ScriptDrops = false;
				spScripter.Options.IncludeHeaders = false;

				var count = 0;
				foreach (ScriptSchemaObjectBase e in db.UserDefinedTableTypes)
				{
					var l = GenerateDbObject(spScripter, e, false, targetDir, "Programmability\\UserDefinedTableTypes", ignoreSchemas);
					if (!string.IsNullOrWhiteSpace(l))
						count++;
				}
				foreach (ScriptSchemaObjectBase e in db.UserDefinedTypes)
				{
					var l = GenerateDbObject(spScripter, e, false, targetDir, "Programmability\\UserDefinedTypes", ignoreSchemas);
					if (!string.IsNullOrWhiteSpace(l))
						count++;
				}
				foreach (ScriptSchemaObjectBase e in db.UserDefinedDataTypes)
				{
					var l = GenerateDbObject(spScripter, e, false, targetDir, "Programmability\\UserDefinedDataTypes", ignoreSchemas);
					if (!string.IsNullOrWhiteSpace(l))
						count++;
				}
				foreach (ScriptSchemaObjectBase e in db.UserDefinedFunctions)
				{
					var l = GenerateDbObject(spScripter, e, false, targetDir, "Programmability\\UserDefinedFunctions", ignoreSchemas);
					if (!string.IsNullOrWhiteSpace(l))
						count++;
				}
				foreach (StoredProcedure e in db.StoredProcedures)
				{
					var l = GenerateDbObject(spScripter, e, false, targetDir, "Programmability\\StoredProcedures", ignoreSchemas);
					if (!string.IsNullOrWhiteSpace(l))
						count++;
				}

				Console.WriteLine($"Programmability Done ({count} objects)");
			}));


			allTasks.Add(Task.Run(() =>
			{
				var sqlConnection = new SqlConnection(dbConnectionString);
				var connection = new ServerConnection(sqlConnection);
				var viewServer = new Server(connection);
				var viewDb = viewServer.Databases[sqlConnection.Database];
				var viewScripter = new Scripter(viewServer);
				viewScripter.Options.ScriptDrops = false;
				viewScripter.Options.IncludeHeaders = false;

				var viewCount = 0;
				foreach (View e in viewDb.Views)
				{
					var location = GenerateDbObject(viewScripter, e, false, targetDir, "Views", ignoreSchemas);
					if(!string.IsNullOrWhiteSpace(location))
						viewCount++;
				}

				Console.WriteLine($"Views Done ({viewCount} views)");
			}));


			var tempSqlConnection = new SqlConnection(dbConnectionString);
			var tempConnection = new ServerConnection(tempSqlConnection);
			var tableNames = new List<KeyValuePair<string, string>>();
			var tableServer = new Server(tempConnection);
			var tableDb = tableServer.Databases[tempSqlConnection.Database];
			foreach (Table t in tableDb.Tables)
			{
				tableNames.Add(new KeyValuePair<string, string>(t.Schema, t.Name));
			}
			
			var threadCount = 5;
			int.TryParse(System.Configuration.ConfigurationManager.AppSettings.Get("GenerateDbObjectsThreadCount"), out threadCount);
			var batchCount = tableNames.Count / threadCount;

			var tempCount = 0;
			var tableTasks = new List<Task>();
			Console.WriteLine($"Starting {threadCount} threads to generate {tableNames.Count} tables in {batchCount} batches");
			for (var i = 0; i <= threadCount; i++)
			{
				var taskTables = tableNames.Skip(tempCount).Take(batchCount).ToList();
				tempCount += batchCount;
				
				tableTasks.Add(Task.Run(() =>
				{
					var tableCount = 0;
					var start = DateTime.Now;
					var sqlConnection = new SqlConnection(dbConnectionString);
					var connection = new ServerConnection(sqlConnection);
					
					var taskServer = new Server(connection);
					var taskDb = tableServer.Databases[sqlConnection.Database];
					var taskScripter = new Scripter(taskServer);
					taskScripter.Options.ScriptDrops = false;
					taskScripter.Options.IncludeHeaders = false;
					taskScripter.Options.Indexes = true;

					foreach (Table e in taskDb.Tables)
					{
						if (!taskTables.Any(o => o.Key == e.Schema && o.Value == e.Name)) continue;

						GenerateDbObject(taskScripter, e, false, targetDir, "Tables", ignoreSchemas);
						tableCount++;
					}

					var end = DateTime.Now;
					Console.WriteLine($"Tables Done (Batch : {i}, Tables : {tableCount}, Time : {end.Subtract(start).TotalSeconds}s)");
				}));

			}
			Task.WaitAll(allTasks.ToArray());
			Task.WaitAll(tableTasks.ToArray());

			var endTime = DateTime.Now;
			Console.WriteLine($"Total time : {endTime.Subtract(startTime).TotalSeconds} seconds");
		}

		private string GenerateDbObject(Scripter scripter,
			ScriptSchemaObjectBase e,
			bool isSystemObject,
			DirectoryInfo targetDir,
			string locationPart,
			IEnumerable<string> ignoreSchemas)
		{
			var name = e.Name;
			var schema = e.Schema;
			if (ignoreSchemas.Contains(schema))
			{
				return null;
			}


			var urn = new[] { e.Urn };
			if (isSystemObject != false) return null;

			var sc = scripter.Script(urn);

			var sb = new StringBuilder();
			foreach (var st in sc)
			{
				sb.Append(" ");
				sb.Append(st);
			}

			var value = sb.ToString().Trim(new[] {'\r', '\n'});
			var location = $"{targetDir.FullName}\\{locationPart}\\{schema}.{name}.sql";
			System.IO.File.WriteAllText(location, value);
			return location;
		}
	}
}
