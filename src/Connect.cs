
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
	// private string? connectionName { get; set; }
	// private string? query { get; set; }
	private Connection connection { get; set; }
	private Command? command { get; set; }

	public Connect (
		string connectionName
	) {
		// this.connectionName = connectionName;
		connection = ConnectionManager.Instance.GetConnection (connectionName, false);
	}

	public Connect (
		Connection connection
	) {
		this.connection = connection;
	}

	public Connect Query (
		string query
	) {
		// this.query = query;
		command = connection.GetCommand (query);
		command.CommandType = CommandType.Text;
		return this;
	}

	public Connect Procedure (
		string query
	) {
		// this.query = query;
		command = connection.GetCommand (query);
		command.CommandType = CommandType.StoredProcedure;
		return this;
	}

	public Connect Function (
		string query
	) {
		// this.query = query;
		command = connection.GetCommand (query);
		command.CommandType = CommandType.StoredProcedure;
		return this;
	}

	public int Go () {
		ArgumentNullException.ThrowIfNull (command);

		_ = connection.OpenAsync ();
		using (connection) {
			using (command) {
				return command.ExecuteNonQueryAsync ().Result;
			}
		}
	}

	// TODO: import Dapper and makes a version of Go like Go<T> ();
	public T Go<T> () {

		ArgumentNullException.ThrowIfNull (command);

		// we have a few things going on here. how to detect if this is just requesting a primitive and not an object?
		var ttype = typeof (T);
		var underlyingType = Nullable.GetUnderlyingType (ttype);
		var isNullable = false;

		if (underlyingType != null) {
			// T is nullable. leave it be
			isNullable = true;
		} else {
			// T is not nullable
			underlyingType = ttype;
		}

		var isValueType = underlyingType.IsValueType;
		var isPrimitive = underlyingType.IsPrimitive; // bool's ttype is primitive, but bool? was not, which is why i'm doing these off the underlying type
		var isString = underlyingType == typeof (string);
		var isBool = underlyingType == typeof (bool);
		var isDate = underlyingType == typeof (DateTime) || underlyingType == typeof (DateTimeOffset);

		var isSingularPrimitive = isPrimitive || isString || isBool || isDate;

		if (isSingularPrimitive) {
			using (connection) {
				using (command) {
					using (var dr = command.ExecuteReader ()) {
						// for primitives, the ordinal will always be the first item
						if (dr.Read ()) {
							var val = dr.GetValue (0);
							if (isDate && ttype == typeof (DateTimeOffset)) {
								// val will convert to a DateTime but not a DatetimeOffset
								return (T)(object)new DateTimeOffset ((DateTime)val);
							}

							return (T)Convert.ChangeType (val, typeof (T));
						}

						// string is always nullable at runtime
						if (isNullable || isString) {
							return default!;
						}

						throw new Exception ("DataReader did not read");
					}
				}
			}
		}

		// var result = connection.BaseConnection.Query<T> (
		// 	command.CommandText
		// );

		// using (connection) {
		// 	using (command) {
		// 		command.BaseCommand.
		// 	}
		// }

		return default!;
	}

	public T Go<T> (
		Func<com.janoserdelyi.DataSource.Command, T> loadObj
	) { //where T : new() {
		ArgumentNullException.ThrowIfNull (command);

		_ = connection.OpenAsync ();
		using (connection) {
			using (command) {
				return loadObj (command);
			}
		}
	}

	public Connect Append (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (command);

		// for mssql do i want to assume nvarchar for this?
		if (connection.DatabaseType == DatabaseType.MSSQL) {
			command.CH.AppendNvarchar (name, value);
			return this;
		}

		command.CH.Append (name, value);
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
		ArgumentNullException.ThrowIfNull (command);

		// for mssql do i want to assume nvarchar for this?
		if (connection.DatabaseType == DatabaseType.MSSQL) {
			command.CH.AppendText (name, value);
			return this;
		}

		command.CH.Append (name, value);
		return this;
	}

	// for mssql
	public Connect Append (
		string name,
		string value,
		int length,
		bool isReturnValue = false
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value, length);
		return this;
	}

	public Connect AppendNvarchar (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.AppendNvarchar (name, value);
		return this;
	}
	public Connect AppendVarchar (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.AppendVarchar (name, value);
		return this;
	}

	public Connect Append (
		string name,
		int value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		long value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTime value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTimeOffset value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTimeOffset? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		short value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		bool value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		byte value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		byte[] value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		TimeSpan value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		Guid value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		string[] value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		int[] value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		int[,] value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		long[] value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		double[] value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTime[] value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		System.Net.IPAddress value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect AppendJson (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.AppendJson (name, value);
		return this;
	}

	public Connect AppendJsonb (
		string name,
		string value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.AppendJsonb (name, value);
		return this;
	}

	public Connect Append (
		string name,
		int? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		long? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		DateTime? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		Guid? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		decimal? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		short? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		bool? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		byte? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		char? value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		System.Collections.BitArray value
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value);
		return this;
	}

	public Connect Append (
		string name,
		System.Collections.BitArray value,
		int size
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value, size);
		return this;
	}

	public Connect Append (
		string name,
		DataTable value,
		string udtTypeName
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Append (name, value, udtTypeName);
		return this;
	}

	public Connect Return (
		string param,
		SqlDbType dbtype
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Return (param, dbtype);
		return this;
	}

	public Connect Return (
		string param,
		SqlDbType dbtype,
		int size
	) {
		ArgumentNullException.ThrowIfNull (command);
		command.CH.Return (param, dbtype, size);
		return this;
	}
}
