using System.Diagnostics.CodeAnalysis;

namespace com.janoserdelyi.DataSource;

public class Connection : DbConnection, IDisposable
{
	#region constructors
	public Connection (
		DbConnection baseConnection,
		DatabaseType databaseType
	) {
		BaseConnection = baseConnection;
		DatabaseType = databaseType;
	}

	public Connection (
		DbConnection baseConnection,
		ConnectionPropertyBag propertyBag
	) {
		ArgumentNullException.ThrowIfNull (baseConnection);
		ArgumentNullException.ThrowIfNull (propertyBag);

		BaseConnection = baseConnection;
		PropertyBag = propertyBag;
		DatabaseType = propertyBag.DatabaseType;
	}
	#endregion

	#region Command
	public Command GetCommand (
		string commandText
	) {
		return new Command (this, commandText);
	}

	internal System.Data.Common.DbCommand getDbCommand (
		string commandText
	) {
		if (string.IsNullOrEmpty (commandText)) {
			throw new System.ArgumentNullException (nameof (commandText));
		}

		//nothing fancy here
		DbCommand command;
		switch (DatabaseType) {
			case DatabaseType.MSSQL:
				//throw new NotSupportedException ("MSSQL removed");
				command = new Microsoft.Data.SqlClient.SqlCommand (commandText, (Microsoft.Data.SqlClient.SqlConnection)BaseConnection) {
					CommandType = CommandType.StoredProcedure
				};
				return command;
			case DatabaseType.Postgresql:
				command = new Npgsql.NpgsqlCommand (commandText, (Npgsql.NpgsqlConnection)BaseConnection) {
					CommandType = CommandType.StoredProcedure
				};
				return command;
			case DatabaseType.MySql:
				command = new MySqlConnector.MySqlCommand (commandText, (MySqlConnector.MySqlConnection)BaseConnection) {
					//command = new MySql.Data.MySqlClient.MySqlCommand (commandText, (MySql.Data.MySqlClient.MySqlConnection)this.baseConnection);
					CommandType = CommandType.StoredProcedure
				};
				return command;
			default:
				throw new System.ArgumentException ("Invalid database type supplied : " + DatabaseType.ToString ());
		}

		throw new System.ArgumentException ("Invalid database type supplied : " + DatabaseType.ToString ());
	}

	public ICommandHelper GetCommandHelper () {
		return DatabaseType switch {
			DatabaseType.MSSQL => new CommandHelperMssql (),//throw new NotSupportedException ("MSSQL removed");
			DatabaseType.Postgresql => new CommandHelperPostgresql (),
			DatabaseType.MySql => new CommandHelperMysql (),
			_ => throw new Exception ("Error finding command helper. '" + DatabaseType.ToString () + "' is not a currently supported database"),
		};
	}
	#endregion

	#region abstract DbConnection inheritor obligations
	public override void Open () {
		if (BaseConnection.State != ConnectionState.Open) {
			BaseConnection.Open ();
		}
	}

	protected override DbCommand CreateDbCommand () {
		return BaseConnection.CreateCommand ();
	}

	public override void ChangeDatabase (
		string databaseName
	) {
		BaseConnection.ChangeDatabase (databaseName);
	}

	//public override event StateChangeEventHandler StateChange;

	public override void Close () {
		if (BaseConnection.State != ConnectionState.Closed) {
			BaseConnection.Close ();
		}
	}

	[AllowNull]
	public override string ConnectionString {
		get {
			return BaseConnection.ConnectionString;
		}
		set {
			BaseConnection.ConnectionString = value;
		}
	}

	public override System.Data.ConnectionState State {
		get { return BaseConnection.State; }
	}

	public override string ServerVersion {
		get { return BaseConnection.ServerVersion; }
	}

	public override string Database {
		get { return BaseConnection.Database; }
	}

	public override string DataSource {
		get { return BaseConnection.DataSource; }
	}

	protected override DbTransaction BeginDbTransaction (
		IsolationLevel isolationLevel
	) {
		return BaseConnection.BeginTransaction (isolationLevel);
	}
	#endregion

	#region IDisposable oblitgations
	public new void Dispose () {
		if (BaseConnection != null && BaseConnection.State != ConnectionState.Closed) {
			BaseConnection.Close ();
		}
	}
	#endregion

	public DbConnection BaseConnection { get; set; }

	public DatabaseType DatabaseType { get; }

	public ConnectionPropertyBag? PropertyBag { get; set; }
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


here's an example of using a user-defined type. basically stuff it with a datatable

var chunkTable = new DataTable ();
chunkTable.Columns.Add ("EmailAddress", typeof (string));
chunkTable.Columns.Add ("ContactId", typeof (int));
chunkTable.Columns.Add ("UpdatedByContactId", typeof (int));
chunkTable.Columns.Add ("ActivitySourceCd", typeof (string));
chunkTable.Columns.Add ("CountryCode", typeof (string));
chunkTable.Columns.Add ("EmailValidationStatusId", typeof (int));
chunkTable.Columns.Add ("LastValidatedDate", typeof (DateTimeOffset));
chunkTable.Columns.Add ("RequestedBy", typeof (string));

foreach (var insertionChunk in insertionChunks) {
	chunkTable.Rows.Clear ();

	foreach (var record in insertionChunk) {
		var row = chunkTable.NewRow ();
		row["EmailAddress"] = record.Emailaddress;
		row["ContactId"] = DBNull.Value;
		row["UpdatedByContactId"] = 207444921;
		row["ActivitySourceCd"] = DBNull.Value;
		row["CountryCode"] = DBNull.Value;
		row["EmailValidationStatusId"] = DBNull.Value;
		row["LastValidatedDate"] = DBNull.Value;
		row["RequestedBy"] = DBNull.Value;
		chunkTable.Rows.Add (row);
	}

	var result = new Connect (connectionName)
		.Query ("exec prefs.uspInsertEmailAddress @pEmailsToInsert;")
		.Append ("pEmailsToInsert", chunkTable, "prefs.InsertEmailAddressType")
		.Go ();
}

*/
