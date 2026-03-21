
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

	public T Go<T> () {
		ArgumentNullException.ThrowIfNull (command);

		if (connection.State != ConnectionState.Open) {
			connection.Open ();
		}

		var dp = new Dapper.DynamicParameters ();
		foreach (DbParameter param in command.BaseCommand.Parameters) {
			if (param is Microsoft.Data.SqlClient.SqlParameter sqlParam && sqlParam.SqlDbType == SqlDbType.Structured) {
				// table-valued parameters need the TypeName preserved; plain dp.Add loses it
				dp.Add (param.ParameterName, ((DataTable)param.Value!).AsTableValuedParameter (sqlParam.TypeName));
			} else {
				dp.Add (param.ParameterName, param.Value, param.DbType);
			}
		}

		var ttype = typeof (T);

		// handle array types (e.g. int[], string[])
		if (ttype.IsArray) {
			var elementType = ttype.GetElementType ()!;

			using (connection) {
				var rows = connection.Query (elementType, command.CommandText, dp).ToList ();
				var array = Array.CreateInstance (elementType, rows.Count);
				for (int i = 0; i < rows.Count; i++) {
					array.SetValue (rows[i], i);
				}

				return (T)(object)array;
			}
		}

		// handle generic collection types (List<T>, IEnumerable<T>, ICollection<T>, IList<T>, etc.)
		if (ttype.IsGenericType) {
			var genericDef = ttype.GetGenericTypeDefinition ();
			bool isCollection =
				genericDef == typeof (List<>) ||
				genericDef == typeof (IEnumerable<>) ||
				genericDef == typeof (ICollection<>) ||
				genericDef == typeof (IList<>) ||
				genericDef == typeof (IReadOnlyList<>) ||
				genericDef == typeof (IReadOnlyCollection<>);

			if (isCollection) {
				var elementType = ttype.GetGenericArguments ()[0];

				using (connection) {
					var rows = connection.Query (elementType, command.CommandText, dp);
					var typedList = (System.Collections.IList)Activator.CreateInstance (typeof (List<>).MakeGenericType (elementType))!;
					foreach (var row in rows) {
						typedList.Add (row);
					}

					return (T)typedList;
				}
			}
		}

		// single value — primitive, string, or complex object
		using (connection) {
			// DateTimeOffset seems to have issues with conversion
			if (ttype == typeof (DateTimeOffset) && connection.PropertyBag?.DatabaseType == DatabaseType.Postgresql) {
				var datetime = connection.QueryFirstOrDefault<DateTime> (command.CommandText, dp)!;
				return (T)(object)new DateTimeOffset (datetime);
			}

			var result = connection.QueryFirstOrDefault<T> (command.CommandText, dp)!;
			return result;
		}
	}

	// public T Go<T> () {

	// 	ArgumentNullException.ThrowIfNull (command);

	// 	// we have a few things going on here. how to detect if this is just requesting a primitive and not an object?
	// 	var ttype = typeof (T);
	// 	var underlyingType = Nullable.GetUnderlyingType (ttype);
	// 	var isNullable = false;

	// 	if (underlyingType != null) {
	// 		// T is nullable. leave it be
	// 		isNullable = true;
	// 	} else {
	// 		// T is not nullable
	// 		underlyingType = ttype;
	// 	}

	// 	var isValueType = underlyingType.IsValueType;
	// 	var isPrimitive = underlyingType.IsPrimitive; // bool's ttype is primitive, but bool? was not, which is why i'm doing these off the underlying type
	// 	var isString = underlyingType == typeof (string);
	// 	var isBool = underlyingType == typeof (bool);
	// 	var isDate = underlyingType == typeof (DateTime) || underlyingType == typeof (DateTimeOffset);
	// 	var isArray = underlyingType.IsArray;
	// 	// var isEnumerable = typeof (IEnumerable<>).IsAssignableFrom (underlyingType) && underlyingType != typeof (string); // string is ienumerable<char>
	// 	var isGenericEnumerable = underlyingType.GetInterfaces ().Any (i => i.IsGenericType && i.GetGenericTypeDefinition () == typeof (IEnumerable<>));

	// 	var isSingularPrimitive = isPrimitive || isString || isBool || isDate;

	// 	using (connection) {
	// 		using (command) {
	// 			using (var dr = command.ExecuteReader ()) {

	// 				if (isSingularPrimitive) {
	// 					// for primitives, the ordinal will always be the first item
	// 					if (dr.Read ()) {
	// 						var val = dr.GetValue (0);

	// 						return returnConvertedType<T> (underlyingType, val, isDate);
	// 					}

	// 					// string is always nullable at runtime
	// 					if (isNullable || isString) {
	// 						return default!;
	// 					}

	// 					return default!;
	// 				}

	// 				if (isArray) {

	// 					var elementType = underlyingType.GetElementType ();

	// 					if (elementType == null) {
	// 						throw new Exception ("error getting element type of array. null returned on inspection");
	// 					}

	// 					var listType = typeof (List<>).MakeGenericType (elementType);
	// 					var list = (List<object>)Activator.CreateInstance (listType)!;

	// 					while (dr.Read ()) {
	// 						var val = dr.GetValue (0);
	// 						list.Add (Convert.ChangeType (val, elementType));
	// 					}

	// 					// Convert list to array
	// 					var array = Array.CreateInstance (elementType, list.Count);
	// 					// list.CopyTo (array, 0);
	// 					return (T)(object)array;
	// 				}

	// 				throw new Exception ("DataReader did not read");
	// 			}
	// 		}
	// 	}
	// }

	// private static T returnConvertedType<T> (
	// 	Type underlyingType,
	// 	object val,
	// 	bool isDate
	// ) {
	// 	if (isDate && underlyingType == typeof (DateTimeOffset)) {
	// 		// val will convert to a DateTime but not a DatetimeOffset
	// 		return (T)(object)new DateTimeOffset ((DateTime)val);
	// 	}

	// 	return (T)Convert.ChangeType (val, typeof (T));
	// }

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
