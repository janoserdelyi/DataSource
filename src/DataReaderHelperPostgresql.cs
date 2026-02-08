using Npgsql;

namespace com.janoserdelyi.DataSource;

public class DataReaderHelperPostgresql : IDataReaderHelper, IDisposable
{
	public DataReaderHelperPostgresql () {

	}

	public DbDataReader? DataReader {
		get { return _dr; }
		set { _dr = (NpgsqlDataReader)value!; }
	}

	public void Dispose () {
		_dr?.Close ();
	}

	public bool HasField (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		// dr.GetOrdinal no longer returns a -1 when the field name is missing. ppbbtthh
		foreach (Npgsql.Schema.NpgsqlDbColumn col in _dr.GetColumnSchema ()) {
			if (col.ColumnName == column) {
				return true;
			}
		}

		return false;
	}

	public string GetString (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetString (ord);
	}
	public string? GetString (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetString (ord);
	}
	/*
	// testing...
	public string Get<T> (
		string column
	) {
		var ord =dr.GetOrdinal (column);

	switch (typeof (T)) {
		case string s:

			break;
	}

}
*/

	// 2022-06-28
	// i'm not convinced i like this. like at all
	// if this is the trash i'm thinking about, i'll just leave it in the client app
	//public System.Numerics.BigInteger GetBigInteger (string column) {
	//	return System.Numerics.BigInteger.Parse (dr.GetString (dr.GetOrdinal (column)));
	//}

	public long GetLong (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetInt64 (ord);
	}
	public long? GetLong (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetInt64 (ord);
	}

	public long GetInt64 (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetInt64 (ord);
	}
	public long? GetInt64 (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetInt64 (ord);
	}

	public double GetDouble (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetDouble (ord);
	}
	public double? GetDouble (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDouble (ord);
	}

	public int GetInt (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetInt32 (ord);
	}
	public int? GetInt (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetInt32 (ord);
	}

	public short GetShort (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetInt16 (ord);
	}
	public short? GetShort (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetInt16 (ord);
	}

	public byte GetByte (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetByte (ord);
	}
	public byte? GetByte (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetByte (ord);
	}

	public byte[] GetByteArray (
		string column,
		int length
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		byte[] buf = new byte[length];

		var ord = _dr.GetOrdinal (column);
		_dr.GetBytes (ord, 0, buf, 0, length);
		return buf;
	}
	public byte[]? GetByteArray (
		string column,
		int length,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		byte[] buf = new byte[length];
		_dr.GetBytes (ord, 0, buf, 0, length);
		return buf;
	}

	public bool GetBool (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetBoolean (ord);
	}
	public bool? GetBool (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetBoolean (ord);
	}

	public DateTime GetDateTime (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		// breaking change with npgsql 6.x - returning datetimekind UTC where i was expecting local previously
		// i need to simply restore that for now but i need to address this better later
		// return dr.GetDateTime (dr.GetOrdinal (column));
		var ord = _dr.GetOrdinal (column);

		DateTime ret = _dr.GetDateTime (ord);

		if (ret.Kind == DateTimeKind.Utc) {
			ret = ret.ToLocalTime ();
		}

		return ret;
	}
	public DateTime? GetDateTime (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		// breaking change with npgsql 6.x - returning datetimekind UTC where i was expecting local previously
		// i need to simply restore that for now but i need to address this better later
		// return dr.GetDateTime (dr.GetOrdinal (column));
		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		DateTime ret = _dr.GetDateTime (ord);

		if (ret.Kind == DateTimeKind.Utc) {
			ret = ret.ToLocalTime ();
		}

		return ret;
	}

	public DateTimeOffset GetDateTimeOffset (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);

		return _dr.GetDateTime (ord);
	}
	public DateTimeOffset? GetDateTimeOffset (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDateTime (ord);
	}

	public TimeSpan GetTimeSpan (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetTimeSpan (ord);
	}
	public TimeSpan? GetTimeSpan (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetTimeSpan (ord);
	}

	public decimal GetDecimal (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetDecimal (ord);
	}
	public decimal? GetDecimal (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDecimal (ord);
	}

	public Guid GetGuid (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetGuid (ord);
	}
	public Guid? GetGuid (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetGuid (ord);
	}

	//added by harv
	public int[]? GetIntArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		return (int[])_dr.GetValue (_dr.GetOrdinal (column));
	}

	public int[,]? GetIntArray2D (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		return (int[,])_dr.GetValue (_dr.GetOrdinal (column));
	}

	public long[]? GetLongArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		return (long[])_dr.GetValue (_dr.GetOrdinal (column));
	}

	public double[]? GetDoubleArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		return (double[])_dr.GetValue (_dr.GetOrdinal (column));
	}

	[Obsolete ("this has not been converted to deal with npgsql 6.x datetime handling changes. use at own risk")]
	public DateTime[]? GetDateTimeArray (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		// warning, these need to return Localtime but they will not since npgsql 6.x
		var rets = (DateTime[])_dr.GetValue (_dr.GetOrdinal (column));

		if (rets != null && rets.Length > 0) {
			var newrets = new DateTime[rets.Length];

			for (var i = 0; i < rets.Length; i++) {
				newrets[i] = rets[i].ToLocalTime ();
			}

			return newrets;
		}

		return rets;
	}

	public string[]? GetStringArray (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);

		// this may no longer be necessary
		if (_dr.GetValue (ord) == DBNull.Value) {
			string[]? val = null;
			return val;
		}

		return _dr.GetFieldValue<string[]> (ord);
	}

	public string GetInet (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetValue (ord).ToString ()!;
	}
	public string? GetInet (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetValue (ord).ToString ();
	}

	// 2016-06-28
	public System.Net.IPAddress GetIpAddress (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return System.Net.IPAddress.Parse (_dr.GetValue (ord).ToString ()!);
	}
	public System.Net.IPAddress? GetIpAddress (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		if (System.Net.IPAddress.TryParse (_dr.GetValue (ord).ToString (), out System.Net.IPAddress? addr)) {
			return addr;
		}

		return null;
	}

	// 2018-04-04
	public System.Collections.BitArray GetBit (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return (System.Collections.BitArray)_dr.GetValue (ord);
	}
	public System.Collections.BitArray? GetBit (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return (System.Collections.BitArray)_dr.GetValue (ord);
	}

	public string GetJson (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetString (ord);
	}
	public string? GetJson (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetString (ord);
	}

	public string GetJsonb (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetString (ord);
	}
	public string? GetJsonb (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetString (ord);
	}

	// 2020-08-24
	public string? GetRegconfig (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetValue (ord).ToString ();
	}

	private NpgsqlDataReader? _dr;
}
