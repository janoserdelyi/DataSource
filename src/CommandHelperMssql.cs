// using System.Text;
using Microsoft.Data.SqlClient;

namespace com.janoserdelyi.DataSource;

public class CommandHelperMssql : ICommandHelper, IDisposable
{

	public CommandHelperMssql () {
		_command = new SqlCommand ();
	}

	public DbCommand Command {
		set { _command = (SqlCommand)value; }
	}

	//added 2008 01 09 janos erdelyi - to allow for IDisposable
	public void Dispose () {
		_command?.Cancel ();
	}

	public void Append (
		string param,
		int value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Int, 4));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		long value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.BigInt, 8));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		short value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.SmallInt, 4));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		byte value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Binary, 1));
		_command.Parameters[param].Value = (byte[])[value];
	}
	public void Append (
		string param,
		byte? value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Binary, 1));
		if (value.HasValue) {
			_command.Parameters[param].Value = (byte[])[value.Value];
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		byte[] value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.VarBinary));
		_command.Parameters[param].Value = value;
	}
	// public void Append (
	// 	string param,
	// 	byte[]? value
	// ) {
	// 	command.Parameters.Add (new SqlParameter (param, SqlDbType.TinyInt));
	// 	if (value == null)
	// 		command.Parameters[param].Value = DBNull.Value;
	// 	else
	// 		command.Parameters[param].Value = value;
	// }

	public void Append (
		string param,
		bool value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Bit, 1));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		char value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Char, 1));
		_command.Parameters[param].Value = value.ToString ();
	}

	public void Append (
		string param,
		decimal value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Decimal));
		_command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		string? value,
		int size
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.VarChar, size));
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
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.NVarChar)); // leaving size out. documents suggest it is inferred from the actual data sent
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
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.VarChar)); // leaving size out. documents suggest it is inferred from the actual data sent
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
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Char, size));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	public void Append (
		string param,
		string value
	) {
		throw new NotImplementedException ();
	}

	public void AppendText (
		string param,
		string value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Text));
		if (value == null) {
			_command.Parameters[param].Value = DBNull.Value;
		} else {
			_command.Parameters[param].Value = value;
		}
	}

	public void Append (
		string param,
		DateTime value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.DateTime, 4));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		DateTimeOffset value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.DateTimeOffset));
		_command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		DateTimeOffset? value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.DateTimeOffset));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		Guid value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.UniqueIdentifier));
		_command.Parameters[param].Value = value;
	}

	public void Append (string param, int[] value) {
		throw new NotImplementedException ();
	}
	public void Append (string param, long[] value) {
		throw new NotImplementedException ();
	}
	public void Append (string param, double[] value) {
		throw new NotImplementedException ();
	}
	public void Append (string param, DateTime[] value) {
		throw new NotImplementedException ();
	}
	public void Append (string param, string[] value) {
		throw new NotImplementedException ();
	}
	public void Append (string param, int[,] value) {
		throw new NotImplementedException ();
	}

	public void AppendNumeric (string param, decimal value) {
		throw new NotImplementedException ();
	}

	// 2013-01-29
	public void AppendInet (string param, string value, int netmask = 32) {
		throw new NotImplementedException ();
	}
	public void Append (string param, System.Net.IPAddress value, int netmask = 32) {
		AppendInet (param, value.ToString ());
	}

	public void Append (
		string param,
		TimeSpan value
	) {
		throw new NotImplementedException ();
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

	public void Append (
		string param,
		int? value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Int, 4));
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
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.BigInt, 8));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		short? value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.SmallInt, 4));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		bool? value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Bit, 1));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		char? value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Char, 1));
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
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Decimal));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		decimal? value,
		bool isMoney
	) {
		if (isMoney) {
			_command.Parameters.Add (new SqlParameter (param, SqlDbType.Money));
		} else {
			_command.Parameters.Add (new SqlParameter (param, SqlDbType.Decimal));
		}

		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		DateTime? value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.DateTime, 4));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		Guid? value
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.UniqueIdentifier));
		if (value.HasValue) {
			_command.Parameters[param].Value = value.Value;
		} else {
			_command.Parameters[param].Value = DBNull.Value;
		}
	}

	public void Append (
		string param,
		DataTable value,
		string udtTypeName
	) {
		_command.Parameters.Add (new SqlParameter (param, SqlDbType.Structured));
		_command.Parameters[param].Value = value;
		// command.Parameters[param].UdtTypeName = udtTypeName;
		_command.Parameters[param].TypeName = udtTypeName;
	}

	public void Return (
		string param,
		SqlDbType dbtype
	) {
		var p = new SqlParameter (param, dbtype) {
			Direction = ParameterDirection.ReturnValue
		};
		_command.Parameters.Add (p);
	}

	public void Return (
		string param,
		SqlDbType dbtype,
		int size
	) {
		var p = new SqlParameter (param, dbtype, size) {
			Direction = ParameterDirection.ReturnValue
		};
		_command.Parameters.Add (p);
	}

	// private string join (char[] ary) {
	// 	var sb = new StringBuilder (8);
	// 	for (int i = 0; i < ary.Length; i++) {
	// 		sb.Append (ary[i]);
	// 	}
	// 	return sb.ToString ();
	// }

	private SqlCommand _command;
}
