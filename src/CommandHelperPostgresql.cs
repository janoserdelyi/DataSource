using Npgsql;
using NpgsqlTypes;

namespace com.janoserdelyi.DataSource;

/// <summary>
/// Summary description for CommandHelper.
/// </summary>
public class CommandHelperPostgresql : ICommandHelper, IDisposable
{
	public CommandHelperPostgresql () {
		_command = new NpgsqlCommand ();
	}

	public DbCommand Command {
		set { _command = (NpgsqlCommand)value; }
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
		//command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Int32, 4));
		//command.Parameters[param].Value = value;

		_command.Parameters.AddWithValue (param, NpgsqlDbType.Integer, value);
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
		//command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Int64, 8));
		//command.Parameters[param].Value = value;

		_command.Parameters.AddWithValue (param, NpgsqlDbType.Bigint, value);
	}

	public void Append (
		string param,
		double value
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Double));
		_command.Parameters[param].Value = value;
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		short value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Int16, 4));
		_command.Parameters[param].Value = value;
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		byte value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Byte));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		byte[] value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Bytea));
		_command.Parameters[param].Value = value;
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		bool value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Boolean, 1));
		_command.Parameters[param].Value = value;
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		char value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.String, 1));
		_command.Parameters[param].Value = value.ToString ();
	}

	public void Append (
		string param,
		decimal value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Decimal));
		_command.Parameters[param].Value = value;
	}

	public void AppendNumeric (
		string param,
		decimal value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Numeric));
		_command.Parameters[param].Value = value;
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	/// <param name="size">size</param>
	public void Append (
		string param,
		string value,
		int size
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.String, size));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	/// <param name="size">size</param>
	public void AppendChar (
		string param,
		string value,
		int size
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.String, size));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void AppendText (
		string param,
		string value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter () {
			ParameterName = param,
			Value = value,
			NpgsqlDbType = NpgsqlDbType.Text
		});
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}
	// 2011-06-13 janos.
	// text is different in postrgres than in MSSQL, so allow this without length definition
	public void Append (
		string param,
		string value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.String));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	// these are just to satisfy the implementation
	public void AppendVarchar (
		string param,
		string value
	) {
		throw new NotImplementedException ("not doing this garbage in postgresql");
	}

	public void AppendNvarchar (
		string param,
		string value
	) {
		throw new NotImplementedException ("not doing this garbage in postgresql");
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		DateTime value
	) {

		//command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.DateTime, 4));
		// npgsql 6.x made breaking changes to timezone with[out] time zone
		if (value.Kind != DateTimeKind.Utc) {
			value = value.ToUniversalTime ();
		}

		_command.Parameters.Add (new Npgsql.NpgsqlParameter {
			ParameterName = param,
			Value = value,
			NpgsqlDbType = NpgsqlDbType.TimestampTz
		});
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		DateTimeOffset value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.DateTimeOffset));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		DateTimeOffset? value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.DateTimeOffset));
		_command.Parameters[param].Value = value;
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		Guid value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Guid));
		_command.Parameters[param].Value = value;
	}

	//added by harv
	public void Append (
		string param,
		int[] value
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Integer));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		long[] value
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Bigint));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		double[] value
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Double));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		DateTime[] value
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Timestamp));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		string[] value
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Varchar));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}
	public void Append (
		string param,
		int[,] value
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Array | NpgsqlDbType.Integer));
		_command.Parameters[param].Value = value;
	}

	// 2013-01-29
	[Obsolete ("use Append(string, IPAddress)")]
	public void AppendInet (
		string param,
		string value,
		int netmask = 32
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Inet));
		//command.Parameters[param].Value = new NpgsqlInet (value);
		_command.Parameters[param].Value = new ValueTuple<System.Net.IPAddress, int> (System.Net.IPAddress.Parse (value), netmask);
	}

	public void Append (
		string param,
		System.Net.IPAddress value,
		int netmask = 32
	) {
		_command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Inet));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			// do i want to try to get the netmask from the broadcast?
			_command.Parameters[param].Value = new ValueTuple<System.Net.IPAddress, int> (value, netmask);
		}
	}

	// 2014-10-05 added
	public void Append (
		string param,
		TimeSpan value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Time));
		_command.Parameters[param].Value = value;
	}

	// 2016-04-02
	public void AppendJson (
		string param,
		string value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Json));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}
	// 2018-07-25
	public void AppendJsonb (
		string param,
		string value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Jsonb));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	// 2018-04-04
	public void Append (
		string param,
		System.Collections.BitArray value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Varbit));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		System.Collections.BitArray value,
		int size
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Bit, size));
		_command.Parameters[param].Value = value;
	}
	#endregion

	#region Nullable<t> Types

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		int? value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Int32, 4));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		long? value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Int64, 8));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		short? value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Int16, 4));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		byte? value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Byte));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		bool? value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Boolean, 1));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		char? value
	) {
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.String, 1));
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
		_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Decimal));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	[Obsolete ("use Append(string, decimal?)")]
	public void Append (
		string param,
		decimal? value,
		bool isMoney
	) {
		if (isMoney) { //TODO:investigate how this impacts postrgres. this is a carryover from MSSQL
			_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, SqlDbType.Money));
		} else {
			_command.Parameters.Add (new Npgsql.NpgsqlParameter (param, DbType.Decimal));
		}

		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		DateTime? value
	) {

		_command.Parameters.Add (new Npgsql.NpgsqlParameter {
			ParameterName = param,
			NpgsqlDbType = NpgsqlDbType.TimestampTz
		});
		//command.Parameters[param].Value = value;

		//command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.DateTime, 4));
		if (value.HasValue) {
			if (value.Value.Kind != DateTimeKind.Utc) {
				value = value.Value.ToUniversalTime ();
			}

			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		Guid? value
	) {
		throw new Exception ("Error, Guids are not yet supported in Npgsql.");
		/*
		command.Parameters.Add(new Npgsql.NpgsqlParameter(param, SqlDbType.UniqueIdentifier));
		command.Parameters[param].Value = value;
		*/
	}

	public void Append (
		string param,
		DataTable value,
		string udtTypeName
	) {
		throw new NotImplementedException ();
		// command.Parameters.Add (new NpgsqlParameter (param, SqlDbType.Udt));
		// command.Parameters[param].Value = value;
	}
	#endregion

	public void Return (
		string param,
		SqlDbType dbtype
	) {
		var p = new Npgsql.NpgsqlParameter (param, dbtype) {
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
		// var p = new Npgsql.NpgsqlParameter (param, dbtype, size);
		// p.Direction = ParameterDirection.ReturnValue;
		// command.Parameters.Add (p);
	}

	private NpgsqlCommand _command;
}
