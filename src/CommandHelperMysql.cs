using MySqlConnector;

namespace com.janoserdelyi.DataSource;

/// <summary>
/// Summary description for CommandHelper.
/// </summary>
public class CommandHelperMysql : ICommandHelper, IDisposable
{
	///<summary>Constructor for CommandHelper</summary>
	public CommandHelperMysql () {
		_command = new MySqlCommand ();
	}
	/*
	public IDbCommand Command {
		set { command = (NpgsqlCommand)value; }
	}
	*/
	public DbCommand Command {
		set { _command = (MySqlCommand)value; }
	}

	//added 2008 01 09 janos erdelyi - to allow for IDisposable
	public void Dispose () {
		_command?.Cancel ();
	}

	#region Regular Types

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		int value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int32, 4));
		_command.Parameters[param].Value = value;
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		long value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int64, 8));
		_command.Parameters[param].Value = value;
	}

	/*
	public void Append (
		string param,
		double value
	) {
		command.Parameters.Add (new MySqlParameter(param, System.Data.DbType.Double));
		command.Parameters[param].Value = value;
	}
	*/
	public void Append (
		string param,
		double value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Double));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		short value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int16, 4));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		byte value
	) {
		_command.Parameters.Add (new MySqlParameter (param, DbType.Byte));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		byte[] value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, SqlDbType.VarBinary));
		_command.Parameters[param].Value = value;
	}

	/// // HOLY CRAP it's the year 2015 and no boolean in mysql?
	/// // i'm going to abuse a bit for this i suppose. or i may just throw an unsupported exception
	public void Append (
		string param,
		bool value
	) {
		//command.Parameters.Add(new MySqlParameter(param, MySqlDbType., 1));
		//command.Parameters[param].Value = value;
		throw new InvalidOperationException ("Mysql does not have a boolean. Please abuse some other data type");
	}

	public void Append (
		string param,
		char value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String, 1));
		_command.Parameters[param].Value = value.ToString ();
	}

	public void Append (
		string param,
		decimal value
	) {
		_command.Parameters.Add (new MySqlParameter (param, DbType.Decimal));
		_command.Parameters[param].Value = value;
	}

	public void AppendNumeric (
		string param,
		decimal value
	) {
		throw new InvalidOperationException ("Not implemented. Append a decimal instead.");
	}

	public void Append (
		string param,
		string value,
		int size
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String, size));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	public void AppendNvarchar (
		string param,
		string value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String)); // leaving size out. documents suggest it is inferred from the actual data sent
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	public void AppendVarchar (
		string param,
		string value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.VarChar)); // leaving size out. documents suggest it is inferred from the actual data sent
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	public void AppendChar (
		string param,
		string value,
		int size
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String, size));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	public void AppendText (
		string param,
		string value
	) {
		_command.Parameters.Add (new MySqlParameter (param, value));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}
	// 2011-06-13 janos.
	// text is different in postrgres than in MSSQL, so allow this without length definition
	public void Append (
		string param,
		string val
	) {
		_command.Parameters.Add (new MySqlParameter (param, DbType.String));
		if (val == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = val;
		}
	}

	public void Append (
		string param,
		DateTime value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.DateTime, 4));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		DateTimeOffset value
	) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	public void Append (
		string param,
		DateTimeOffset? value
	) {
		throw new InvalidOperationException ("Not yet implemented.");
	}

	public void Append (
		string param,
		Guid value
	) {
		_command.Parameters.Add (new MySqlParameter (param, DbType.Guid));
		_command.Parameters[param].Value = value;
	}

	//added by harv
	public void Append (string param, int[] value) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	public void Append (string param, long[] value) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	public void Append (string param, double[] value) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	public void Append (string param, DateTime[] value) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	public void Append (string param, string[] value) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	public void Append (string param, int[,] value) {
		throw new InvalidOperationException ("Not yet implemented.");
	}

	// 2013-01-29
	public void AppendInet (string param, string value, int netmask = 32) {
		throw new InvalidOperationException ("Not yet implemented.");
	}

	public void Append (string param, System.Net.IPAddress value, int netmask = 32) {
		AppendInet (param, value.ToString ());
	}

	// 2014-10-05 added
	public void Append (
		string param,
		TimeSpan value
	) {
		throw new InvalidOperationException ("Not yet implemented.");
	}

	// 2016-04-02
	public void AppendJson (
		string param,
		string value
	) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	// 2018-07-25
	public void AppendJsonb (
		string param,
		string value
	) {
		throw new InvalidOperationException ("Not yet implemented.");
	}

	// 2018-04-04
	public void Append (
		string param,
		System.Collections.BitArray value
	) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	public void Append (
		string param,
		System.Collections.BitArray value,
		int size
	) {
		throw new InvalidOperationException ("Not yet implemented.");
	}
	#endregion

	#region Nullable<t> Types
	public void Append (
		string param,
		int? value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int32, 4));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		long? value
	) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int64, 8));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (string param, short? value) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int16, 4));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (string param, byte? value) {
		_command.Parameters.Add (new MySqlParameter (param, DbType.Byte));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (string param, bool? value) {
		/*
		command.Parameters.Add(new MySqlParameter(param, System.Data.DbType.Boolean, 1));
		if (value.HasValue){
			command.Parameters[param].Value = value.Value;
		}else{
			command.Parameters[param].Value = DBNull.Value;
		}
		*/
		throw new InvalidOperationException ("Mysql does not have a boolean. Please abuse some other data type");
	}

	public void Append (string param, char? value) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String, 1));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		decimal? value
	) {
		_command.Parameters.Add (new MySqlParameter (param, DbType.Decimal));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (string param, decimal? value, bool isMoney) {
		if (isMoney) { //TODO:investigate how this impacts postrgres. this is a carryover from MSSQL
			_command.Parameters.Add (new MySqlParameter (param, SqlDbType.Money));
		} else {
			_command.Parameters.Add (new MySqlParameter (param, DbType.Decimal));
		}

		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (string param, DateTime? value) {
		_command.Parameters.Add (new MySqlParameter (param, MySqlDbType.DateTime, 4));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (string param, Guid? value) {
		throw new Exception ("Error, Not yet implemented.");
		/*
		command.Parameters.Add(new MySqlParameter(param, SqlDbType.UniqueIdentifier));
		command.Parameters[param].Value = value;
		*/
	}

	public void Append (
		string param,
		DataTable value,
		string udtTypeName
	) {
		throw new NotImplementedException ();
	}
	#endregion

	public void Return (
		string param,
		SqlDbType dbtype
	) {
		var p = new MySqlParameter (param, dbtype) {
			Direction = ParameterDirection.ReturnValue
		};
		_command.Parameters.Add (p);
	}

	public void Return (
		string param,
		SqlDbType dbtype,
		int size
	) {
		throw new NotImplementedException ();
		// var p = new MySqlParameter (param, dbtype, size);
		// p.Direction = ParameterDirection.ReturnValue;
		// command.Parameters.Add (p);
	}

	private MySqlCommand _command;
}
