using System;
using System.Collections.Generic;
using System.Text;

namespace com.janoserdelyi.DataSource
{

	public class ConnectionPropertyBag
	{
		public ConnectionPropertyBag () {

		}

		//added 2007 12 30 janos erdelyi. this keeps things from being parsed EVERY time
		public static ConnectionPropertyBag GetBag (
			string connectionString, //this is the original string passed in
			DatabaseType dbType
		) {

			if (!parsedStrings.ContainsKey (connectionString)) {
				parsedStrings.Add (connectionString, Parse (connectionString, dbType));
			}

			return parsedStrings[connectionString];
		}

		public DatabaseType DatabaseType { get; set; }
		public string Name { get; set; }
		public string Server { get; set; }
		public string Database { get; set; }
		public string Username { get; set; }
		public string Password { get; set; }
		public string Passfile { get; set; }
		public string Port { get; set; }
		public string Encoding { get; set; }
		public bool Pooling { get; set; }
		public string MaxPoolSize { get; set; }
		public string MinPoolSize { get; set; }
		public string Timeout { get; set; }
		// 2014-08-01 janos. added
		public string CommandTimeout { get; set; }
		public string ApplicationName { get; set; }
		public string Keepalive { get; set; }
		// 2023-03-13
		public bool IncludeErrorDetail { get; set; }

		private static IDictionary<string, ConnectionPropertyBag> parsedStrings { get; set; } = new Dictionary<string, ConnectionPropertyBag> ();


		public override string ToString () {
			//no db type, no play. throw an exception
			//i used to allow a default for older projects. older projects will be using an older dll.

			switch (this.DatabaseType) {
				case DatabaseType.MSSQL:
					return makeMssqlConnectionString ();
				case DatabaseType.Postgresql:
					return makePostgresqlConnectionString ();
				case DatabaseType.MySql:
					return makeMysqlConnectionString ();
				default:
					throw new ArgumentException (this.DatabaseType.ToString () + "  is not a supported Database type");
			}
		}

		public static ConnectionPropertyBag Parse (
			string connectionString,
			DatabaseType dbType
		) {
			if (connectionString == null) {
				//throw new System.ArgumentNullException("a connection string is required");
				throw new System.ArgumentNullException ("connectionString");
			}
			if (connectionString == "") {
				throw new System.ArgumentException ("the connection string may not be empty");
			}

			ConnectionPropertyBag bag = new ConnectionPropertyBag () {
				DatabaseType = dbType
			};

			// 2018-11-13. trim out spaces after semi-colons.
			connectionString = System.Text.RegularExpressions.Regex.Replace (connectionString, ";\\s+", ";");

			//the name is original string passed in
			bag.Name = connectionString;

			if (dbType == DatabaseType.MSSQL) {
				/*	take what i get an try to get these parts:
					server[,<port>];database;[uid;pwd;]
				*/
				string[] parts = connectionString.Split (';');
				foreach (string part in parts) {
					//ghetto time!

					// 2018-11-13 overwrite the bag name if one is suplied in the connection string
					if (part.ToLower ().StartsWith ("name", StringComparison.CurrentCultureIgnoreCase)) {
						bag.Name = part.Split ('=')[1];
					}

					if (part.ToLower ().StartsWith ("server", StringComparison.CurrentCultureIgnoreCase)) {
						//this is most variable one of the group
						//it can be server=xxx.xxx.xxx.xxx;
						//or server=xxx.xxx.xxx.xxx,yyyyy; where y is the port
						if (part.Contains (",")) {
							bag.Server = part.Split (',')[0].Substring ("server=".Length);
							bag.Port = part.Split (',')[1];
						} else {
							bag.Server = part.Split ('=')[1];
						}
					}
					if (part.ToLower ().StartsWith ("database", StringComparison.CurrentCultureIgnoreCase)) {
						bag.Database = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("uid", StringComparison.CurrentCultureIgnoreCase)) {
						bag.Username = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("pwd", StringComparison.CurrentCultureIgnoreCase)) {
						bag.Password = part.Split ('=')[1];
					}

					if (part.ToLower ().StartsWith ("user id", StringComparison.CurrentCultureIgnoreCase)) {
						bag.Username = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("password", StringComparison.CurrentCultureIgnoreCase)) {
						bag.Password = part.Split ('=')[1];
					}

					if (part.ToLower ().StartsWith ("max pool size", StringComparison.CurrentCultureIgnoreCase)) {
						bag.MaxPoolSize = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("min pool size", StringComparison.CurrentCultureIgnoreCase)) {
						bag.MinPoolSize = part.Split ('=')[1];
					}

					//2011-09-09. needed to control timeout. my connect was crapping and the TeamUp list import was crapping out
					if (part.ToLower ().StartsWith ("connection timeout", StringComparison.CurrentCultureIgnoreCase)) {
						bag.Timeout = part.Split ('=')[1];
					}

					//2012-08-01. to help identify distinct connections in the database
					if (part.ToLower ().StartsWith ("application name", StringComparison.CurrentCultureIgnoreCase)) {
						bag.ApplicationName = part.Split ('=')[1];
					}


					//yep, i seem to be discarding everything else.
					//TODO: think about how to add on all the other potential options in the connection string
				}
			}

			if (dbType == DatabaseType.Postgresql) {
				// example:
				// Server=192.168.1.61;Port=5432;User Id=merryfool;Password=pitythefool;Database=merryfool_db;encoding=unicode;
				string[] parts = connectionString.Split (new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

				foreach (string part in parts) {

					if (part.IndexOf ("=") == -1) {
						Console.WriteLine ("invalid connectionstring segement '" + part + "'");
						continue;
					}

					string label = part.Split ('=')[0].ToLower ();
					string value = part.Split ('=')[1];

					switch (label) {
						case "name":
							bag.Name = value; // 2018-11-13 overwrite the bag name if one is suplied in the connection string
							break;
						case "server":
							bag.Server = value;
							break;
						case "port":
							bag.Port = value;
							break;
						case "database":
							bag.Database = value;
							break;
						case "username":
							bag.Username = value;
							break;
						case "password":
							bag.Password = value;
							break;
						case "passfile":
							bag.Passfile = value;
							break;
						case "client encoding":
							bag.Encoding = value;
							break;
						case "pooling":
							if (bool.TryParse (value, out bool p)) {
								bag.Pooling = p;
							} else {
								bag.Pooling = true; // default
							}
							break;
						case "maxpoolsize":
							bag.MaxPoolSize = value;
							break;
						case "minpoolsize":
							bag.MinPoolSize = value;
							break;
						case "command timeout":
							bag.CommandTimeout = value;
							break;
						case "timeout":
							bag.Timeout = value;
							break;
						case "application name":
							bag.ApplicationName = value;
							break;
						case "includeerrordetail":
							if (bool.TryParse (value, out bool ied)) {
								bag.IncludeErrorDetail = ied;
							} else {
								bag.IncludeErrorDetail = false; // default
							}
							break;
					}
				}
			}

			if (dbType == DatabaseType.MySql) {
				//example:
				//server=50.56.110.52;port=3306;database=manhattan_ewbtesst;uid=dobiesync;pwd=dobiedoo;
				string[] parts = connectionString.Split (';');
				foreach (string part in parts) {
					// 2018-11-13 overwrite the bag name if one is suplied in the connection string
					if (part.ToLower ().StartsWith ("name", StringComparison.CurrentCultureIgnoreCase)) {
						bag.Name = part.Split ('=')[1];
					}

					if (part.ToLower ().StartsWith ("server")) {
						bag.Server = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("port")) {
						bag.Port = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("database")) {
						bag.Database = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("uid")) {
						bag.Username = part.Split ('=')[1];
					}
					// also accepts 'user'
					if (part.ToLower ().StartsWith ("uid")) {
						bag.Username = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("pwd")) {
						bag.Password = part.Split ('=')[1];
					}
					// also accepts 'password'
					if (part.ToLower ().StartsWith ("password")) {
						bag.Password = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("charset")) {
						bag.Encoding = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("connection timeout")) {
						bag.Timeout = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("maximumpoolsize")) {
						bag.MaxPoolSize = part.Split ('=')[1];
					}
					if (part.ToLower ().StartsWith ("minimumpoolsize")) {
						bag.MinPoolSize = part.Split ('=')[1];
					}
				}
			}

			return bag;
		}


		private string makeMssqlConnectionString () {
			/*
			if (database.ToLower() == "logging") {
				throw new Exception("invalid dbtype detection '" + dbtype.ToString() + "' for connection '" + database + "'");
			}
			*/
			StringBuilder sb = new StringBuilder ();

			//server
			sb.Append ("server=").Append (Server);
			if (Port != null && !Port.Equals ("1433")) {
				sb.Append (",").Append (Port);
			}
			sb.Append (";");

			//database
			sb.Append ("database=").Append (Database).Append (";");

			//username
			if (!string.IsNullOrEmpty (Username)) {
				sb.Append ("uid=").Append (Username).Append (";pwd=").Append (Password);
			} else {
				sb.Append ("Integrated Security=SSPI");
			}
			sb.Append (";");

			if (MinPoolSize != null) {
				sb.Append ("Min Pool Size=").Append (MinPoolSize).Append (";");
			}

			if (MaxPoolSize != null) {
				sb.Append ("Max Pool Size=").Append (MaxPoolSize).Append (";");
			}

			if (Timeout != null) {
				sb.Append ("Connection Timeout=").Append (Timeout).Append (";");
			}

			if (ApplicationName != null) {
				sb.Append ("Application Name=").Append (ApplicationName).Append (";");
			}

			//"App=HCL;"
			//"Connection Reset=false;"

			// temp. i need flags for this
			sb.Append ("Encrypt=false;");
			// another option is 
			// TrustServerCertificate=True

			//other features will be added as i actuall use them. i don't think i'm going
			//to try to cover a bunch of cases that i don't actually encounter in production

			return sb.ToString ();
		}

		private string makePostgresqlConnectionString () {

			//example:
			//"Server=192.168.1.61;Port=5432;User Id=merryfool;Password=pitythefool;Database=merryfool_db;encoding=unicode;"
			//"Server=127.0.0.1;Port=59000;User Id=postgres;Password=doggiedoo;Database=logging;""
			//2007 11 18 janos erdelyi
			//adding encoding=unicode to the string
			//i need to really add encoding as an option

			//testing pooling issues
			//Pooling : true or false
			//MinPoolSize : default 1
			//MaxPoolSize : default 2

			// 2019-12-30 https://www.npgsql.org/doc/connection-string-parameters.html

			StringBuilder sb = new StringBuilder ();

			//server
			sb.Append ("Server=").Append (Server).Append (";");

			//port
			if (Port != null) {
				sb.Append ("Port=").Append (Port).Append (";");
			}


			//database
			sb.Append ("Database=").Append (Database).Append (";");

			// NOTE:
			// there are a few auth methods in postgresql
			// i am simply writing out this method to handle the one i am currently using.
			// when i begin using other types, i will expand on this.

			// username
			if (!string.IsNullOrEmpty (Username)) {
				sb.Append ("Username=").Append (Username).Append (";");
			}
			// password
			if (!string.IsNullOrEmpty (Password) && string.IsNullOrEmpty (Passfile)) {
				sb.Append ("Password=").Append (Password).Append (";");
			}
			if (!string.IsNullOrEmpty (Passfile)) {
				sb.Append ("Passfile=").Append (Passfile).Append (";");
			}

			// Gets or sets the client_encoding parameter. Since 3.1.
			if (Encoding == null) {
				//sb.Append("Client Encoding=UTF8;");
			} else {
				sb.Append ("Client Encoding=").Append (Encoding).Append (";");
			}

			//sb.Append("Pooling=True;");
			//sb.Append("Pooling=").Append(pooling.ToString().ToLower()).Append(";");

			if (MinPoolSize == null) {
				//sb.Append("MinPoolSize=1;");
			} else {
				sb.Append ("MinPoolSize=").Append (MinPoolSize).Append (";");
			}

			if (MaxPoolSize == null) {
				//sb.Append("MaxPoolSize=40;");
			} else {
				sb.Append ("MaxPoolSize=").Append (MaxPoolSize).Append (";");
			}

			// The time to wait (in seconds) while trying to establish a connection before terminating the attempt and generating an error. default 15
			if (!string.IsNullOrEmpty (Timeout)) {
				sb.Append ("Timeout=").Append (Timeout).Append (";");
			}

			// The time to wait (in seconds) while trying to execute a command before terminating the attempt and generating an error. Set to zero for infinity. default 30
			if (!string.IsNullOrEmpty (CommandTimeout)) {
				sb.Append ("Command Timeout=").Append (CommandTimeout).Append (";");
			}

			// The number of seconds of connection inactivity before Npgsql sends a keepalive query. default disabled
			if (!string.IsNullOrEmpty (Keepalive)) {
				sb.Append ("Keepalive=").Append (Keepalive).Append (";");
			}

			if (!string.IsNullOrEmpty (ApplicationName)) {
				sb.Append ("Application Name=").Append (ApplicationName).Append (";");
			}

			if (IncludeErrorDetail == true) {
				sb.Append ("Include Error Detail=True;");
			}

			string temp = sb.ToString ();

			return sb.ToString ();
		}

		private string makeMysqlConnectionString () {

			//example:
			//server=50.56.110.52;port=3306;database=manhattan_ewbtesst;uid=dobiesync;pwd=dobiedoo;

			StringBuilder sb = new StringBuilder ();

			//server
			sb.Append ("server=").Append (Server).Append (";");

			//port
			if (Port != null) {
				sb.Append ("port=").Append (Port).Append (";");
			}

			//database
			sb.Append ("database=").Append (Database).Append (";");

			//username
			if (Username != null && Username.Length > 0) {
				sb.Append ("uid=").Append (Username).Append (";");
			}
			//password
			if (Password != null && Password.Length > 0) {
				sb.Append ("pwd=").Append (Password).Append (";");
			}

			//supposedly ignore these days
			if (Encoding == null) {
				//sb.Append("Encoding=UTF8;");
			} else {
				sb.Append ("charset=").Append (Encoding).Append (";");
			}

			//sb.Append("Pooling=True;");


			if (MinPoolSize == null) {
				//sb.Append("MinPoolSize=1;");
			} else {
				sb.Append ("minimumpoolsize=").Append (MinPoolSize).Append (";");
			}

			if (MaxPoolSize == null) {
				//sb.Append("MaxPoolSize=40;");
			} else {
				sb.Append ("maximumpoolsize=").Append (MaxPoolSize).Append (";");
			}

			// timeout. in seconds not milliseconds
			if (!string.IsNullOrEmpty (Timeout)) {
				sb.Append ("connection timeout=").Append (Timeout).Append (";");
			}

			// commandtimeout. in seconds not milliseconds

			string temp = sb.ToString ();

			return sb.ToString ();
		}


	}
}
