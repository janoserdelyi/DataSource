using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using io = System.IO;
using System.Xml;

namespace com.janoserdelyi.DataSource
{
	public class ConnectionManager
	{

		private ConnectionManager () {
			connections = new ConcurrentDictionary<string, ConnectionPropertyBag> ();
			instanceCreateDate = DateTime.Now;
		}

		public Connection GetConnection (
			string name,
			bool returnOpenConnection = true
		) {
			if (string.IsNullOrEmpty (name)) {
				throw new ArgumentNullException ("name");
			}

			if (!connections.ContainsKey (name)) {
				throw new ArgumentException ("'" + name + "' is not a valid Connection name. This is case-sensitive.");
			}

			DbConnection connection = null;

			switch (connections[name].DatabaseType) {
				case DatabaseType.MSSQL:
					connection = new System.Data.SqlClient.SqlConnection (connections[name].ToString ());
					break;
				case DatabaseType.Postgresql:
					connection = new Npgsql.NpgsqlConnection (connections[name].ToString ());
					break;
				case DatabaseType.MySql:
					connection = new MySql.Data.MySqlClient.MySqlConnection (connections[name].ToString ());
					break;
				case DatabaseType.Sqlite:
					throw new NotSupportedException ("Sqlite is not yet supported");
				default:
					throw new ArgumentException ("Unknown database type. Not defaulting to anything.");
			}

			Connection c = new Connection (connection, connections[name]);

			if (returnOpenConnection) {
				c.Open ();
			}

			return c;
		}


		#region public properties
		public static ConnectionManager Instance {
			get {
				if (instance == null) {
					instance = new ConnectionManager ();
					try {
						instance.initialize ();
					} catch (MissingConfigurationException) {
						throw;
					} catch (MalformedConfigurationException) {
						throw;
					} catch (Exception oops) {
						throw new Exception ("Error initializing instance of ConnectionManager", oops);
					}
				}

				return instance;
			}
		}

		public IDictionary<string, ConnectionPropertyBag> Connections {
			get {
				//i've hit cases where this get called before initialize. so check!
				if (instance == null) {
					ConnectionManager caller = ConnectionManager.Instance; //just call the get
				}

				return connections;
			}
		}

		public DateTime InstanceCreateDate {
			get { return instanceCreateDate; }
		}
		#endregion

		// 2018-11-13 how do i not have this yet? will make testing easier
		public void AddConnection (
			ConnectionPropertyBag bag
		) {
			if (bag == null) {
				throw new ArgumentNullException ("bag");
			}
			// could do some error-checking for required properties, but eh. you don't add you get to live with it
			instance.connections.GetOrAdd (bag.Name, bag);
		}

		#region private methods
		private void initialize (

		) {

			// TODO: put a filesystemwatcher on the config file and reload if it changes

			XmlDocument config = new XmlDocument ();
			try {
				config.Load (io.Path.Combine (AppDomain.CurrentDomain.BaseDirectory, "DataSource.config"));
			} catch (System.IO.FileNotFoundException) { // oops) {
														//throw new MissingConfigurationException("Unable to locate DataSource.config at '" + AppDomain.CurrentDomain.BaseDirectory + "'", oops);

				// 2015-04-03 a bit of a shift from previous history.
				// i don't want it to be required to have a config file since connects can be programatically added
				// put up a non-blocking warning of some sort instead

				System.Diagnostics.Debug.WriteLine ("Unable to locate DataSource.config at '" + AppDomain.CurrentDomain.BaseDirectory + "'");

				// 2017-03-07 i will have it search just a little though.
				// there are plenty of occasions where this is looking in bin/ but i want to step back one directory
				string rootFolder = AppDomain.CurrentDomain.BaseDirectory;
				System.IO.DirectoryInfo di = new System.IO.DirectoryInfo (rootFolder);
				if (di.Name.ToLower () == "bin") {
					//Console.WriteLine(di.Parent.FullName);
					rootFolder = di.Parent.FullName;
				}
				bool loaded = false;
				try {
					config.Load (io.Path.Combine (rootFolder, "DataSource.config"));
					loaded = true;
				} catch (System.IO.FileNotFoundException) {
					System.Diagnostics.Debug.WriteLine ("Unable to locate DataSource.config at '" + rootFolder + "'");
					return;
				}
				if (!loaded) {
					return;
				}

			} catch (Exception oops) {
				throw new MalformedConfigurationException ("Error in DataSource.config : " + oops.Message, oops);
			}

			XmlNodeList connectionNodeList = config.SelectNodes ("/configuration/connections/connection");

			foreach (XmlNode connectionNode in connectionNodeList) {
				ConnectionPropertyBag connectionPropertyBag = new ConnectionPropertyBag ();

				string dbtype = connectionNode.SelectSingleNode ("dbtype").InnerText;
				if (string.IsNullOrEmpty (dbtype)) {
					throw new ArgumentException ("Unknown database type in config.");
				}

				DatabaseType databaseType;
				switch (dbtype.ToLower ()) {
					case "postgresql":
						databaseType = DatabaseType.Postgresql;
						break;
					case "mssql":
						databaseType = DatabaseType.MSSQL;
						break;
					case "mysql":
						databaseType = DatabaseType.MySql;
						break;
					//throw new NotSupportedException("MySql is not yet supported");
					case "sqlite":
						//databaseType = DatabaseType.Sqlite;
						//break;
						throw new NotSupportedException ("Sqlite is not yet supported");
					default:
						throw new ArgumentException ("Unknown database type. Not defaulting to anything.");
				}

				connectionPropertyBag.DatabaseType = databaseType;
				connectionPropertyBag.Name = connectionNode.SelectSingleNode ("name").InnerText;
				connectionPropertyBag.Server = connectionNode.SelectSingleNode ("server").InnerText;
				connectionPropertyBag.Database = connectionNode.SelectSingleNode ("database").InnerText;
				connectionPropertyBag.Username = connectionNode.SelectSingleNode ("username").InnerText;
				connectionPropertyBag.Password = connectionNode.SelectSingleNode ("password").InnerText;
				connectionPropertyBag.Port = connectionNode.SelectSingleNode ("port").InnerText;

				if (connectionNode.SelectSingleNode ("maxpoolsize") != null) {
					if (int.TryParse (connectionNode.SelectSingleNode ("maxpoolsize").InnerText, out int maxpool)) {
						connectionPropertyBag.MaxPoolSize = maxpool.ToString ();
					}
				}
				if (connectionNode.SelectSingleNode ("minpoolsize") != null) {
					if (int.TryParse (connectionNode.SelectSingleNode ("minpoolsize").InnerText, out int minpool)) {
						connectionPropertyBag.MinPoolSize = minpool.ToString ();
					}
				}
				if (connectionNode.SelectSingleNode ("encoding") != null) {
					connectionPropertyBag.Encoding = connectionNode.SelectSingleNode ("encoding").InnerText;
				}
				if (connectionNode.SelectSingleNode ("timeout") != null) {
					connectionPropertyBag.Timeout = connectionNode.SelectSingleNode ("timeout").InnerText;
				}
				if (connectionNode.SelectSingleNode ("commandtimeout") != null) {
					connectionPropertyBag.CommandTimeout = connectionNode.SelectSingleNode ("commandtimeout").InnerText;
				}
				if (connectionNode.SelectSingleNode ("applicationname") != null) {
					connectionPropertyBag.ApplicationName = connectionNode.SelectSingleNode ("applicationname").InnerText;
				}

				// i'm not changing the value if it exists, so i'm just passing in a Func that gets me the value i want in the first place
				// instance.connections.AddOrUpdate(connectionPropertyBag.Name, connectionPropertyBag, (key, oldValue) => connectionPropertyBag);
				// the name of this isn't intuitive for what i want but functionally it matches more closely
				instance.connections.GetOrAdd (connectionPropertyBag.Name, connectionPropertyBag);
			}
		}
		#endregion

		private static ConnectionManager instance = null;
		private ConcurrentDictionary<string, com.janoserdelyi.DataSource.ConnectionPropertyBag> connections = null; //keyed by Name
		private DateTime instanceCreateDate;

	}
}