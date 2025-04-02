namespace com.janoserdelyi.DataSource;

public class Connection : DbConnection, IDisposable
{
	#region constructors
	public Connection (
		DbConnection baseConnection,
		DatabaseType databaseType
	) {
		this.baseConnection = baseConnection;
		this.databaseType = databaseType;
	}

	public Connection (
		DbConnection baseConnection,
		ConnectionPropertyBag propertyBag
	) {
		ArgumentNullException.ThrowIfNull (baseConnection);
		ArgumentNullException.ThrowIfNull (propertyBag);

		this.baseConnection = baseConnection;
		this.propertyBag = propertyBag;
		this.databaseType = propertyBag.DatabaseType;
	}
	#endregion

	#region Command
	public Command GetCommand (
		string commandText
	) {
		return new Command (this, commandText);
	}

	internal System.Data.Common.DbCommand GetDbCommand (
		string commandText
	) {
		if (string.IsNullOrEmpty (commandText)) {
			throw new System.ArgumentNullException (nameof (commandText));
		}

		//nothing fancy here
		DbCommand command;
		switch (this.databaseType) {
			case DatabaseType.MSSQL:
				//throw new NotSupportedException ("MSSQL removed");
				command = new Microsoft.Data.SqlClient.SqlCommand (commandText, (Microsoft.Data.SqlClient.SqlConnection)this.baseConnection);
				command.CommandType = CommandType.StoredProcedure;
				return command;
			case DatabaseType.Postgresql:
				command = new Npgsql.NpgsqlCommand (commandText, (Npgsql.NpgsqlConnection)this.baseConnection);
				command.CommandType = CommandType.StoredProcedure;
				return command;
			case DatabaseType.MySql:
				command = new MySqlConnector.MySqlCommand (commandText, (MySqlConnector.MySqlConnection)this.baseConnection);
				//command = new MySql.Data.MySqlClient.MySqlCommand (commandText, (MySql.Data.MySqlClient.MySqlConnection)this.baseConnection);
				command.CommandType = CommandType.StoredProcedure;
				return command;
			default:
				throw new System.ArgumentException ("Invalid database type supplied : " + databaseType.ToString ());
		}

		throw new System.ArgumentException ("Invalid database type supplied : " + databaseType.ToString ());
	}

	public ICommandHelper GetCommandHelper () {
		switch (this.databaseType) {
			case DatabaseType.MSSQL:
				//throw new NotSupportedException ("MSSQL removed");
				return new CommandHelperMssql ();
			case DatabaseType.Postgresql:
				return new CommandHelperPostgresql ();
			case DatabaseType.MySql:
				return new CommandHelperMysql ();
			default:
				throw new Exception ("Error finding command helper. '" + databaseType.ToString () + "' is not a currently supported database");
		}
	}
	#endregion

	#region abstract DbConnection inheritor obligations
	public override void Open () {
		if (baseConnection.State != ConnectionState.Open) {
			baseConnection.Open ();
		}
	}

	protected override DbCommand CreateDbCommand () {
		return baseConnection.CreateCommand ();
	}

	public override void ChangeDatabase (
		string databaseName
	) {
		baseConnection.ChangeDatabase (databaseName);
	}

	//public override event StateChangeEventHandler StateChange;

	public override void Close () {
		if (baseConnection.State != ConnectionState.Closed) {
			baseConnection.Close ();
		}
	}

	public override string ConnectionString {
		get {
			return baseConnection.ConnectionString;
		}
		set {
			baseConnection.ConnectionString = value;
		}
	}

	public override System.Data.ConnectionState State {
		get { return baseConnection.State; }
	}

	public override string ServerVersion {
		get { return baseConnection.ServerVersion; }
	}

	public override string Database {
		get { return baseConnection.Database; }
	}

	public override string DataSource {
		get { return baseConnection.DataSource; }
	}

	protected override DbTransaction BeginDbTransaction (
		IsolationLevel isolationLevel
	) {
		return baseConnection.BeginTransaction (isolationLevel);
	}
	#endregion

	#region IDisposable oblitgations
	public new void Dispose () {
		if (baseConnection != null && baseConnection.State != ConnectionState.Closed) {
			baseConnection.Close ();
		}
	}
	#endregion

	public DbConnection BaseConnection {
		get { return baseConnection; }
		set { baseConnection = value; }
	}

	public DatabaseType DatabaseType {
		get { return databaseType; }
	}

	public ConnectionPropertyBag? PropertyBag {
		get { return propertyBag; }
		set { propertyBag = value; }
	}

	private DbConnection baseConnection;
	private readonly DatabaseType databaseType;
	private ConnectionPropertyBag? propertyBag;
}

/*
thinking about adding a bulk copy mechanism, but it may require a few things

like on the model object, a proiperty indexer may be needed.
i would bet this could be sped up but here goes :

public object? this[string name] {
	get {
		var properties = typeof (MyModelObject).GetProperties (BindingFlags.Public | BindingFlags.Instance);

		foreach (var property in properties) {
			if (property.Name == name && property.CanRead) {
				return property.GetValue (this, null);
			}
		}

		throw new ArgumentException ("Can't find property");
	}
	set {
		return;
	}
}

then a method to convert to a DataTable :

public static DataTable ConvertToDataTable (
	List<MyModelObject> records
) {
	var props = TypeDescriptor.GetProperties (typeof (MyModelObject));

	DataTable dt = new DataTable ();

	foreach (PropertyDescriptor p in props) {
		dt.Columns.Add (p.Name, p.PropertyType.BaseType ?? p.PropertyType);
	}

	foreach (var record in records) {
		var row = dt.NewRow ();

		foreach (PropertyDescriptor p in props) {
			row[p.Name] = record[p.Name];
		}

		dt.Rows.Add (row);
	}

	return dt;
}

then actual usage :

var dt = MyModelObject.ConvertToDataTable (records);

using (SqlConnection cn = new SqlConnection (ConnectionManager.Instance.GetConnection (connectionName).ConnectionString)) {
	cn.Open ();
	using (SqlBulkCopy bulkCopy = new SqlBulkCopy (cn)) {
		bulkCopy.DestinationTableName = "prefs.MyModelObject";
		bulkCopy.WriteToServer (dt);
	}
	cn.Close ();
}

i think this implementation is naive and needs to be looked at more

*/
