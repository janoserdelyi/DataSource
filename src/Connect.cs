namespace com.janoserdelyi.DataSource;

public class Connect
{
	// i wonder if i could produce structures to allow for syntax like :
	// insert -
	// new Connect("connection name").Query("insert into...").Append("param1Name", param1Value).Append("param2Name", param2Value).Go();

	// select? not sure how i would want to distingush looping results vs not. ICollection detection?
	// SomeReturnType ret = new Connect("connection name").Query("insert into...").Append("param1Name", param1Value).Append("param2Name", param2Value).Go<return type>();

	/*

	*/
	private string? connectionName { get; set; }
	private string? query { get; set; }
	private Connection connection { get; set; }
	private Command? command { get; set; }

	public Connect (
		string connectionName
	) {
		this.connectionName = connectionName;
		this.connection = ConnectionManager.Instance.GetConnection (connectionName, false);
	}

	public Connect (
		Connection connection
	) {
		this.connection = connection;
	}

	public Connect Query (
		string query
	) {
		this.query = query;
		this.command = this.connection.GetCommand (query);
		this.command.CommandType = CommandType.Text;
		return this;
	}

	public Connect Procedure (
		string query
	) {
		this.query = query;
		this.command = this.connection.GetCommand (query);
		this.command.CommandType = CommandType.StoredProcedure;
		return this;
	}

	public Connect Function (
		string query
	) {
		this.query = query;
		this.command = this.connection.GetCommand (query);
		this.command.CommandType = CommandType.StoredProcedure;
		return this;
	}

	public int Go () {
		ArgumentNullException.ThrowIfNull (this.command);

		this.connection.OpenAsync ();
		using (this.connection) {
			using (this.command) {
				return this.command.ExecuteNonQueryAsync ().Result;
			}
		}
	}

	// i'm considering requiring an interface on this
	// and likely take a func<> for loading the object
	public T Go<T> (
		Func<com.janoserdelyi.DataSource.Command, T> loadObj
	) { //where T : new() {
		ArgumentNullException.ThrowIfNull (this.command);

		this.connection.OpenAsync ();
		using (this.connection) {
			using (this.command) {
				return loadObj (this.command);
			}
		}
	}

	public Connect Append (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (this.command);

		// for mssql do i want to assume nvarchar for this?
		if (this.connection.DatabaseType == DatabaseType.MSSQL) {
			this.command.CH.AppendNvarchar (name, value);
			return this;
		}

		this.command.CH.Append (name, value);
		return this;
	}

	// public Connect Append (
	// 	string name,
	// 	string? value
	// ) {
	// 	ArgumentNullException.ThrowIfNull (this.command);

	// 	// for mssql do i want to assume nvarchar for this?
	// 	if (this.connection.DatabaseType == DatabaseType.MSSQL) {
	// 		this.command.CH.AppendNvarchar (name, value);
	// 		return this;
	// 	}

	// 	this.command.CH.Append (name, value);
	// 	return this;
	// }

	[Obsolete ("text datatype is deprecated as of sql server 2016")]
	public Connect AppendText (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (this.command);

		// for mssql do i want to assume nvarchar for this?
		if (this.connection.DatabaseType == DatabaseType.MSSQL) {
			this.command.CH.AppendText (name, value);
			return this;
		}

		this.command.CH.Append (name, value);
		return this;
	}

	// for mssql
	public Connect Append (
		string name,
		string value,
		int length,
		bool isReturnValue = false
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value, length);
		return this;
	}

	public Connect AppendNvarchar (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.AppendNvarchar (name, value);
		return this;
	}
	public Connect AppendVarchar (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.AppendVarchar (name, value);
		return this;
	}

	public Connect Append (
		string name,
		int value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		long value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTime value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTimeOffset value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTimeOffset? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		short value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		bool value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		byte value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		byte[] value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		TimeSpan value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		Guid value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		string[] value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		int[] value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		int[,] value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		long[] value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		double[] value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTime[] value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		System.Net.IPAddress value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect AppendJson (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.AppendJson (name, value);
		return this;
	}

	public Connect AppendJsonb (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.AppendJsonb (name, value);
		return this;
	}

	public Connect Append (
		string name,
		int? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		long? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTime? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		Guid? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		decimal? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		short? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		bool? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		byte? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		char? value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		System.Collections.BitArray value
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		System.Collections.BitArray value,
		int size
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value, size);
		return this;
	}

	public Connect Append (
		string name,
		DataTable value,
		string udtTypeName
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Append (name, value, udtTypeName);
		return this;
	}

	public Connect Return (
		string param,
		SqlDbType dbtype
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Return (param, dbtype);
		return this;
	}

	public Connect Return (
		string param,
		SqlDbType dbtype,
		int size
	) {
		ArgumentNullException.ThrowIfNull (this.command);
		this.command.CH.Return (param, dbtype, size);
		return this;
	}

}
