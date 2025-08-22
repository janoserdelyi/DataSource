using System.Text;

namespace com.janoserdelyi.DataSource;

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
		if (!parsedStrings.ContainsKey (connectionString)) {
			parsedStrings.Add (connectionString, Parse (connectionString, dbType));
		}

		return parsedStrings[connectionString];
	}

	public DatabaseType DatabaseType { get; set; }
	public string? Name { get; set; }
	public string? Server { get; set; }
	public string? Database { get; set; }
	public string? Username { get; set; }
	public string? Password { get; set; }
	public string? Passfile { get; set; }
	public string? Port { get; set; } // TODO: really should be an int
	public string? Encoding { get; set; }
	public bool? Pooling { get; set; }
	public string? MaxPoolSize { get; set; }
	public string? MinPoolSize { get; set; }
	public string? Timeout { get; set; } // TODO: really should be an int
										 // 2014-08-01 janos. added
	public string? CommandTimeout { get; set; }// TODO: really should be an int
	public string? ApplicationName { get; set; }
	public string? Keepalive { get; set; }
	// 2023-03-13
	public bool? IncludeErrorDetail { get; set; }
	// 2024-10-29
	public string? IntegratedSecurity { get; set; }
	public bool? Encrypt { get; set; }
	public bool? TrustedConnection { get; set; }

	// SSL/Security parameters for PostgreSQL
	public string? SslMode { get; set; }
	public bool? TrustServerCertificate { get; set; }
	public string? SslCertificate { get; set; }
	public string? SslKey { get; set; }
	public string? SslPassword { get; set; }
	public string? RootCertificate { get; set; }
	public bool? CheckCertificateRevocation { get; set; }

	// Performance parameters for PostgreSQL
	public string? MaxAutoPrepare { get; set; }
	public string? AutoPrepareMinUsages { get; set; }
	public string? ReadBufferSize { get; set; }
	public string? WriteBufferSize { get; set; }
	public bool? NoResetOnClose { get; set; }

	// Connection management parameters for PostgreSQL
	public string? ConnectionIdleLifetime { get; set; }
	public string? ConnectionPruningInterval { get; set; }
	public string? ConnectionLifetime { get; set; }
	public bool? Enlist { get; set; }

	// Advanced feature parameters for PostgreSQL
	public string? SearchPath { get; set; }
	public string? Timezone { get; set; }
	public string? Options { get; set; }
	public bool? LogParameters { get; set; }

	private static Dictionary<string, ConnectionPropertyBag> parsedStrings { get; set; } = new Dictionary<string, ConnectionPropertyBag> ();

	private static readonly char[] separator = new char[] { ';' };

	public override string ToString () {
		//no db type, no play. throw an exception
		//i used to allow a default for older projects. older projects will be using an older dll.

		switch (this.DatabaseType) {
			case com.janoserdelyi.DataSource.DatabaseType.MSSQL:
				return makeMssqlConnectionString ();
			case com.janoserdelyi.DataSource.DatabaseType.Postgresql:
				return makePostgresqlConnectionString ();
			case com.janoserdelyi.DataSource.DatabaseType.MySql:
				return makeMysqlConnectionString ();
			default:
				throw new ArgumentException (this.DatabaseType.ToString () + "  is not a supported Database type");
		}
	}

	public static ConnectionPropertyBag Parse (
		string connectionString,
		DatabaseType dbType
	) {
		ArgumentNullException.ThrowIfNull (connectionString);

		if (connectionString == "") {
			throw new System.ArgumentException ("the connection string may not be empty");
		}

		var bag = new ConnectionPropertyBag () {
			DatabaseType = dbType
		};

		// 2018-11-13. trim out spaces after semi-colons.
		connectionString = System.Text.RegularExpressions.Regex.Replace (connectionString, ";\\s+", ";");

		//the name is original string passed in
		bag.Name = connectionString;

		if (dbType == com.janoserdelyi.DataSource.DatabaseType.MSSQL) {
			/*	take what i get an try to get these parts:
				server[,<port>];database;[uid;pwd;]
			*/
			string[] parts = connectionString.Split (';');

			foreach (string part in parts) {

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
				if (part.ToLower ().StartsWith ("user id", StringComparison.CurrentCultureIgnoreCase)) {
					bag.Username = part.Split ('=')[1];
				}
				if (part.ToLower ().StartsWith ("pwd", StringComparison.CurrentCultureIgnoreCase)) {
					bag.Password = part.Split ('=')[1];
				}
				if (part.ToLower ().StartsWith ("password", StringComparison.CurrentCultureIgnoreCase)) {
					bag.Password = part.Split ('=')[1];
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

				// 2024-10-29
				if (part.ToLower ().StartsWith ("integrated security", StringComparison.CurrentCultureIgnoreCase)) {
					bag.IntegratedSecurity = part.Split ('=')[1];
				}
				if (part.ToLower ().StartsWith ("encrypt", StringComparison.CurrentCultureIgnoreCase)) {
					bag.Encrypt = part.Split ('=')[1].ToLower () == "true" ? true : false;
				}
				if (part.ToLower ().StartsWith ("trusted_connection", StringComparison.CurrentCultureIgnoreCase)) {
					bag.TrustedConnection = part.Split ('=')[1].ToLower () == "true" ? true : false;
				}

				//yep, i seem to be discarding everything else.
				//TODO: think about how to add on all the other potential options in the connection string
			}
		}

		if (dbType == com.janoserdelyi.DataSource.DatabaseType.Postgresql) {
			// example:
			// Server=192.168.1.61;Port=5432;User Id=merryfool;Password=pitythefool;Database=merryfool_db;encoding=unicode;
			string[] parts = connectionString.Split (separator, StringSplitOptions.RemoveEmptyEntries);

			foreach (string part in parts) {

				if (!part.Contains ("=", StringComparison.CurrentCulture)) {
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
					case "host":
						bag.Server = value;
						break;
					case "port":
						bag.Port = value;
						break;
					case "database":
						bag.Database = value;
						break;
					case "username":
					case "user id":
						bag.Username = value;
						break;
					case "password":
						bag.Password = value;
						break;
					case "passfile":
						bag.Passfile = value;
						break;
					case "client encoding":
					case "encoding":
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
					case "maximum pool size":
						bag.MaxPoolSize = value;
						break;
					case "minpoolsize":
					case "minimum pool size":
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
					case "keepalive":
						bag.Keepalive = value;
						break;
					case "includeerrordetail":
					case "include error detail":
						if (bool.TryParse (value, out bool ied)) {
							bag.IncludeErrorDetail = ied;
						} else {
							bag.IncludeErrorDetail = false; // default
						}
						break;

					// SSL/Security parameters
					case "ssl mode":
					case "sslmode":
						bag.SslMode = value;
						break;
					case "trust server certificate":
					case "trustservercertificate":
						if (bool.TryParse (value, out bool tsc)) {
							bag.TrustServerCertificate = tsc;
						}
						break;
					case "ssl certificate":
					case "sslcert":
						bag.SslCertificate = value;
						break;
					case "ssl key":
					case "sslkey":
						bag.SslKey = value;
						break;
					case "ssl password":
					case "sslpassword":
						bag.SslPassword = value;
						break;
					case "root certificate":
					case "sslrootcert":
						bag.RootCertificate = value;
						break;
					case "check certificate revocation":
					case "checkcertificaterevocation":
						if (bool.TryParse (value, out bool ccr)) {
							bag.CheckCertificateRevocation = ccr;
						}
						break;

					// Performance parameters
					case "max auto prepare":
					case "maxautoprepare":
						bag.MaxAutoPrepare = value;
						break;
					case "auto prepare min usages":
					case "autoprepareminusages":
						bag.AutoPrepareMinUsages = value;
						break;
					case "read buffer size":
					case "readbuffersize":
						bag.ReadBufferSize = value;
						break;
					case "write buffer size":
					case "writebuffersize":
						bag.WriteBufferSize = value;
						break;
					case "no reset on close":
					case "noresetonclose":
						if (bool.TryParse (value, out bool nroc)) {
							bag.NoResetOnClose = nroc;
						}
						break;

					// Connection management parameters
					case "connection idle lifetime":
					case "connectionidlelifetime":
						bag.ConnectionIdleLifetime = value;
						break;
					case "connection pruning interval":
					case "connectionpruninginterval":
						bag.ConnectionPruningInterval = value;
						break;
					case "connection lifetime":
					case "connectionlifetime":
						bag.ConnectionLifetime = value;
						break;
					case "enlist":
						if (bool.TryParse (value, out bool enlist)) {
							bag.Enlist = enlist;
						}
						break;

					// Advanced feature parameters
					case "search path":
					case "searchpath":
						bag.SearchPath = value;
						break;
					case "timezone":
					case "tz":
						bag.Timezone = value;
						break;
					case "options":
						bag.Options = value;
						break;
					case "log parameters":
					case "logparameters":
						if (bool.TryParse (value, out bool lp)) {
							bag.LogParameters = lp;
						}
						break;
				}
			}
		}

		if (dbType == com.janoserdelyi.DataSource.DatabaseType.MySql) {
			//example:
			//server=50.56.110.52;port=3306;database=manhattan_ewbtesst;uid=dobiesync;pwd=dobiedoo;
			string[] parts = connectionString.Split (';');
			foreach (string part in parts) {

				string partcompare = part.ToLower ();

				// 2018-11-13 overwrite the bag name if one is suplied in the connection string
				if (partcompare.StartsWith ("name")) {
					bag.Name = part.Split ('=')[1];
				}

				if (partcompare.StartsWith ("server")) {
					bag.Server = part.Split ('=')[1];
				}
				if (partcompare.StartsWith ("port")) {
					bag.Port = part.Split ('=')[1];
				}
				if (partcompare.StartsWith ("database")) {
					bag.Database = part.Split ('=')[1];
				}
				if (partcompare.StartsWith ("uid")) {
					bag.Username = part.Split ('=')[1];
				}
				// also accepts 'user'
				if (partcompare.StartsWith ("uid")) {
					bag.Username = part.Split ('=')[1];
				}
				if (partcompare.StartsWith ("pwd")) {
					bag.Password = part.Split ('=')[1];
				}
				// also accepts 'password'
				if (partcompare.StartsWith ("password")) {
					bag.Password = part.Split ('=')[1];
				}
				if (partcompare.StartsWith ("charset")) {
					bag.Encoding = part.Split ('=')[1];
				}
				if (partcompare.StartsWith ("connection timeout")) {
					bag.Timeout = part.Split ('=')[1];
				}
				if (partcompare.StartsWith ("maximumpoolsize")) {
					bag.MaxPoolSize = part.Split ('=')[1];
				}
				if (partcompare.StartsWith ("minimumpoolsize")) {
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
		var sb = new StringBuilder ();

		sb.Append ($"server={Server}");
		if (Port != null && !Port.Equals ("1433")) {
			sb.Append (',').Append (Port);
		}
		sb.Append (';');

		sb.Append ($"database={Database};");

		if (!string.IsNullOrEmpty (Username)) {
			sb.Append ($"uid={Username};pwd={Password};");
		}

		if (MinPoolSize != null) {
			sb.Append ($"Min Pool Size={MinPoolSize};");
		}

		if (MaxPoolSize != null) {
			sb.Append ($"Max Pool Size={MaxPoolSize};");
		}

		if (Timeout != null) {
			sb.Append ($"Connection Timeout={Timeout};");
		}

		if (!string.IsNullOrEmpty (CommandTimeout)) {
			sb.Append ($"Command Timeout={CommandTimeout};");
		}

		if (ApplicationName != null) {
			sb.Append ($"Application Name={ApplicationName};");
		}

		//"App=HCL;"
		//"Connection Reset=false;"
		//"App=HCL;"
		//"Connection Reset=false;"

		if (IntegratedSecurity != null) {
			sb.Append ($"Integrated Security={IntegratedSecurity};");
		}
		if (Encrypt != null) {
			sb.Append ($"Encrypt={(Encrypt.Value ? "true" : "false")};");
		}
		if (TrustedConnection != null) {
			sb.Append ($"Trusted_Connection={(TrustedConnection.Value ? "true" : "false")};");
		}

		// another option is
		// TrustServerCertificate=True

		//other features will be added as i actually use them. i don't think i'm going
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

		var sb = new StringBuilder ();

		//server
		sb.Append ($"Server={Server};");

		//port
		if (Port != null) {
			sb.Append ($"Port={Port};");
		}


		//database
		sb.Append ($"Database={Database};");

		// NOTE:
		// there are a few auth methods in postgresql
		// i am simply writing out this method to handle the one i am currently using.
		// when i begin using other types, i will expand on this.

		// username
		if (!string.IsNullOrEmpty (Username)) {
			sb.Append ($"Username={Username};");
		}
		// password
		if (!string.IsNullOrEmpty (Password) && string.IsNullOrEmpty (Passfile)) {
			sb.Append ($"Password={Password};");
		}
		if (!string.IsNullOrEmpty (Passfile)) {
			sb.Append ($"Passfile={Passfile};");
		}

		// Gets or sets the client_encoding parameter. Since 3.1.
		if (Encoding == null) {
			//sb.Append("Client Encoding=UTF8;");
		} else {
			sb.Append ($"Client Encoding={Encoding};");
		}

		//sb.Append("Pooling=True;");
		//sb.Append("Pooling=").Append(pooling.ToString().ToLower()).Append(";");

		if (MinPoolSize == null) {
			//sb.Append("MinPoolSize=1;");
		} else {
			sb.Append ($"MinPoolSize={MinPoolSize};");
		}

		if (MaxPoolSize == null) {
			//sb.Append("MaxPoolSize=40;");
		} else {
			sb.Append ($"MaxPoolSize={MaxPoolSize};");
		}

		// The time to wait (in seconds) while trying to establish a connection before terminating the attempt and generating an error. default 15
		if (!string.IsNullOrEmpty (Timeout)) {
			sb.Append ($"Timeout={Timeout};");
		}

		// The time to wait (in seconds) while trying to execute a command before terminating the attempt and generating an error. Set to zero for infinity. default 30
		if (!string.IsNullOrEmpty (CommandTimeout)) {
			sb.Append ($"Command Timeout={CommandTimeout};");
		}

		// The number of seconds of connection inactivity before Npgsql sends a keepalive query. default disabled
		if (!string.IsNullOrEmpty (Keepalive)) {
			sb.Append ($"Keepalive={Keepalive};");
		}

		if (!string.IsNullOrEmpty (ApplicationName)) {
			sb.Append ($"Application Name={ApplicationName};");
		}

		if (IncludeErrorDetail == true) {
			sb.Append ("Include Error Detail=True;");
		}

		// SSL/Security parameters
		if (!string.IsNullOrEmpty (SslMode)) {
			sb.Append ($"SSL Mode={SslMode};");
		}
		if (TrustServerCertificate == true) {
			sb.Append ("Trust Server Certificate=True;");
		}
		if (!string.IsNullOrEmpty (SslCertificate)) {
			sb.Append ($"SSL Certificate={SslCertificate};");
		}
		if (!string.IsNullOrEmpty (SslKey)) {
			sb.Append ($"SSL Key={SslKey};");
		}
		if (!string.IsNullOrEmpty (SslPassword)) {
			sb.Append ($"SSL Password={SslPassword};");
		}
		if (!string.IsNullOrEmpty (RootCertificate)) {
			sb.Append ($"Root Certificate={RootCertificate};");
		}
		if (CheckCertificateRevocation == true) {
			sb.Append ("Check Certificate Revocation=True;");
		}

		// Performance parameters
		if (!string.IsNullOrEmpty (MaxAutoPrepare)) {
			sb.Append ($"Max Auto Prepare={MaxAutoPrepare};");
		}
		if (!string.IsNullOrEmpty (AutoPrepareMinUsages)) {
			sb.Append ($"Auto Prepare Min Usages={AutoPrepareMinUsages};");
		}
		if (!string.IsNullOrEmpty (ReadBufferSize)) {
			sb.Append ($"Read Buffer Size={ReadBufferSize};");
		}
		if (!string.IsNullOrEmpty (WriteBufferSize)) {
			sb.Append ($"Write Buffer Size={WriteBufferSize};");
		}

		// Connection Management parameters
		if (!string.IsNullOrEmpty (ConnectionIdleLifetime)) {
			sb.Append ($"Connection Idle Lifetime={ConnectionIdleLifetime};");
		}
		if (!string.IsNullOrEmpty (ConnectionLifetime)) {
			sb.Append ($"Connection Lifetime={ConnectionLifetime};");
		}
		if (Enlist == false) {
			sb.Append ("Enlist=False;");
		}

		// Advanced Feature parameters
		if (!string.IsNullOrEmpty (SearchPath)) {
			sb.Append ($"Search Path={SearchPath};");
		}
		if (!string.IsNullOrEmpty (Timezone)) {
			sb.Append ($"Timezone={Timezone};");
		}
		if (!string.IsNullOrEmpty (Options)) {
			sb.Append ($"Options={Options};");
		}
		if (LogParameters == true) {
			sb.Append ("Log Parameters=True;");
		}
		if (NoResetOnClose == true) {
			sb.Append ("No Reset On Close=True;");
		}

		string temp = sb.ToString ();

		return sb.ToString ();
	}

	private string makeMysqlConnectionString () {

		//example:
		//server=50.56.110.52;port=3306;database=manhattan_ewbtesst;uid=dobiesync;pwd=dobiedoo;

		var sb = new StringBuilder ();

		//server
		sb.Append ($"server={Server};");

		//port
		if (Port != null) {
			sb.Append ($"port={Port};");
		}

		//database
		sb.Append ($"database={Database};");

		//username
		if (Username != null && Username.Length > 0) {
			sb.Append ($"uid={Username};");
		}
		//password
		if (Password != null && Password.Length > 0) {
			sb.Append ($"pwd={Password};");
		}

		//supposedly ignore these days
		if (Encoding == null) {
			//sb.Append("Encoding=UTF8;");
		} else {
			sb.Append ($"charset={Encoding};");
		}

		//sb.Append("Pooling=True;");

		if (MinPoolSize == null) {
			//sb.Append("MinPoolSize=1;");
		} else {
			sb.Append ($"minimumpoolsize={MinPoolSize};");
		}

		if (MaxPoolSize == null) {
			//sb.Append("MaxPoolSize=40;");
		} else {
			sb.Append ($"maximumpoolsize={MaxPoolSize};");
		}

		// timeout. in seconds not milliseconds
		if (!string.IsNullOrEmpty (Timeout)) {
			sb.Append ($"connection timeout={Timeout};");
		}

		// commandtimeout. in seconds not milliseconds

		return sb.ToString ();
	}
}
