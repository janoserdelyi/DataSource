namespace com.janoserdelyi.DataSource;

public class Connection : DbConnection, IDisposable {
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
