using System.Collections.Concurrent;
using System.Xml;

namespace com.janoserdelyi.DataSource;

public class ConnectionManager
{

	private ConnectionManager () {
		_connections = new ConcurrentDictionary<string, ConnectionPropertyBag> ();
		InstanceCreateDate = DateTime.Now;
	}

	public Connection GetConnection (
		string name,
		bool returnOpenConnection = true
	) {
		ArgumentException.ThrowIfNullOrEmpty (name);

		if (!_connections.TryGetValue (name, out ConnectionPropertyBag? value)) {
			throw new ArgumentException ($"'{name}' is not a valid Connection name. This is case-sensitive.");
		}

		DbConnection?
				//throw new NotSupportedException ("MSSQL removed");
				connection = value.DatabaseType switch {
					DatabaseType.MSSQL => new Microsoft.Data.SqlClient.SqlConnection (value.ToString ()),//throw new NotSupportedException ("MSSQL removed");
					DatabaseType.Postgresql => new Npgsql.NpgsqlConnection (value.ToString ()),
					DatabaseType.MySql => new MySqlConnector.MySqlConnection (value.ToString ()),
					DatabaseType.Sqlite => throw new NotSupportedException ("Sqlite is not yet supported"),
					_ => throw new ArgumentException ("Unknown database type. Not defaulting to anything."),
				};
		var c = new Connection (connection, value);

		if (returnOpenConnection) {
			c.Open ();
		}

		return c;
	}

	#region public properties
	public static ConnectionManager Instance {
		get {
			if (_instance == null) {
				_instance = new ConnectionManager ();
				try {
					_instance.initialize ();
				} catch (MissingConfigurationException) {
					throw;
				} catch (MalformedConfigurationException) {
					throw;
				} catch (Exception oops) {
					throw new Exception ("Error initializing instance of ConnectionManager", oops);
				}
			}

			return _instance;
		}
	}

	public IDictionary<string, ConnectionPropertyBag> Connections {
		get {
			//i've hit cases where this get called before initialize. so check!
			if (_instance == null) {
				_ = Instance; //just call the getter
			}

			return _connections;
		}
	}

	public DateTime InstanceCreateDate { get; }
	#endregion

	// 2018-11-13 how do i not have this yet? will make testing easier
	public void AddConnection (
		ConnectionPropertyBag bag
	) {
		ArgumentNullException.ThrowIfNull (bag);
		ArgumentNullException.ThrowIfNull (bag.Name);

		if (_instance == null) {
			throw new NullReferenceException ("'instance' is null somehow. what kind of database are you adding a connection to?");
		}

		// could do some error-checking for required properties, but eh. you don't add you get to live with it
		_instance._connections.GetOrAdd (bag.Name, bag);
	}

	#region private methods
	private void initialize (

	) {
		// TODO: put a filesystemwatcher on the config file and reload if it changes

		var config = new XmlDocument ();
		try {
			config.Load (Path.Combine (AppDomain.CurrentDomain.BaseDirectory, "DataSource.config"));
		} catch (FileNotFoundException) { // oops) {
										  //throw new MissingConfigurationException("Unable to locate DataSource.config at '" + AppDomain.CurrentDomain.BaseDirectory + "'", oops);

			// 2015-04-03 a bit of a shift from previous history.
			// i don't want it to be required to have a config file since connects can be programatically added
			// put up a non-blocking warning of some sort instead

			System.Diagnostics.Debug.WriteLine ("Unable to locate DataSource.config at '" + AppDomain.CurrentDomain.BaseDirectory + "'");

			// 2017-03-07 i will have it search just a little though.
			// there are plenty of occasions where this is looking in bin/ but i want to step back one directory
			string rootFolder = AppDomain.CurrentDomain.BaseDirectory;

			var di = new DirectoryInfo (rootFolder);

			if (di == null) {
				System.Diagnostics.Debug.WriteLine ($"Unable to get DirectoryInfo for folder '{rootFolder}'");
				return;
			}

			if (di.Name.Equals ("bin", StringComparison.CurrentCultureIgnoreCase)) {
				if (di.Parent == null) {
					System.Diagnostics.Debug.WriteLine ($"Unable to get parent directory for folder '{rootFolder}'");
					return;
				}

				rootFolder = di.Parent.FullName;
			}

			bool loaded = false;
			try {
				config.Load (Path.Combine (rootFolder, "DataSource.config"));
				loaded = true;
			} catch (FileNotFoundException) {
				System.Diagnostics.Debug.WriteLine ($"Unable to locate DataSource.config at '{rootFolder}'");
				return;
			}

			if (!loaded) {
				return;
			}
		} catch (Exception oops) {
			throw new MalformedConfigurationException ("Error in DataSource.config : " + oops.Message, oops);
		}

		XmlNodeList? connectionNodeList = config.SelectNodes ("/configuration/connections/connection");

		if (connectionNodeList == null) {
			throw new MalformedConfigurationException ("DataSource.config loaded, but no <connection/> sections found");
		}

		foreach (XmlNode connectionNode in connectionNodeList) {
			var connectionPropertyBag = new ConnectionPropertyBag ();

			string? dbtype = connectionNode.SelectSingleNode ("dbtype")?.InnerText;

			if (string.IsNullOrEmpty (dbtype)) {
				throw new ArgumentException ("Unknown database type in config.");
			}

			var databaseType = dbtype.ToLower () switch {
				"postgresql" => DatabaseType.Postgresql,
				"mssql" => DatabaseType.MSSQL,
				"mysql" => DatabaseType.MySql,
				"sqlite" => throw new NotSupportedException ("Sqlite is not yet supported"),//databaseType = DatabaseType.Sqlite;
				_ => throw new ArgumentException ("Unknown database type. Not defaulting to anything."),
			};
			connectionPropertyBag.DatabaseType = databaseType;
			connectionPropertyBag.Name = connectionNode.SelectSingleNode ("name")!.InnerText;
			connectionPropertyBag.Server = connectionNode.SelectSingleNode ("server")!.InnerText;
			connectionPropertyBag.Database = connectionNode.SelectSingleNode ("database")!.InnerText;
			connectionPropertyBag.Username = connectionNode.SelectSingleNode ("username")?.InnerText;
			connectionPropertyBag.Password = connectionNode.SelectSingleNode ("password")?.InnerText;
			connectionPropertyBag.Port = connectionNode.SelectSingleNode ("port")?.InnerText;

			if (connectionNode.SelectSingleNode ("maxpoolsize") != null) {
				if (int.TryParse (connectionNode.SelectSingleNode ("maxpoolsize")?.InnerText, out int maxpool)) {
					connectionPropertyBag.MaxPoolSize = maxpool.ToString ();
				}
			}

			if (connectionNode.SelectSingleNode ("minpoolsize") != null) {
				if (int.TryParse (connectionNode.SelectSingleNode ("minpoolsize")?.InnerText, out int minpool)) {
					connectionPropertyBag.MinPoolSize = minpool.ToString ();
				}
			}

			if (connectionNode.SelectSingleNode ("encoding") != null) {
				connectionPropertyBag.Encoding = connectionNode.SelectSingleNode ("encoding")?.InnerText;
			}

			if (connectionNode.SelectSingleNode ("timeout") != null) {
				connectionPropertyBag.Timeout = connectionNode.SelectSingleNode ("timeout")?.InnerText;
			}

			if (connectionNode.SelectSingleNode ("commandtimeout") != null) {
				connectionPropertyBag.CommandTimeout = connectionNode.SelectSingleNode ("commandtimeout")?.InnerText;
			}

			if (connectionNode.SelectSingleNode ("applicationname") != null) {
				connectionPropertyBag.ApplicationName = connectionNode.SelectSingleNode ("applicationname")?.InnerText;
			}

			if (connectionNode.SelectSingleNode ("encrypt") != null) {
				connectionPropertyBag.Encrypt = bool.Parse (connectionNode.SelectSingleNode ("encrypt")?.InnerText!);
			}

			if (connectionNode.SelectSingleNode ("trustedconnection") != null) {
				connectionPropertyBag.TrustedConnection = bool.Parse (connectionNode.SelectSingleNode ("trustedconnection")?.InnerText!);
			}

			if (connectionNode.SelectSingleNode ("integratedsecurity") != null) {
				connectionPropertyBag.IntegratedSecurity = connectionNode.SelectSingleNode ("integratedsecurity")?.InnerText!;
			}

			// i'm not changing the value if it exists, so i'm just passing in a Func that gets me the value i want in the first place
			// instance.connections.AddOrUpdate(connectionPropertyBag.Name, connectionPropertyBag, (key, oldValue) => connectionPropertyBag);
			// the name of this isn't intuitive for what i want but functionally it matches more closely
			if (_instance == null) {
				throw new NullReferenceException ("'instance' is null somehow. what kind of database are you adding a connection to?");
			}

			_instance._connections.GetOrAdd (connectionPropertyBag.Name, connectionPropertyBag);
		}
	}
	#endregion

	private static ConnectionManager? _instance;
	private readonly ConcurrentDictionary<string, com.janoserdelyi.DataSource.ConnectionPropertyBag> _connections = new (); //keyed by Name

}
