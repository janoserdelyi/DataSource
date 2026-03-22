using Npgsql;

namespace com.janoserdelyi.DataSource;

public class DataReaderHelperPostgresql : DataReaderHelperBase
{
	public DataReaderHelperPostgresql () {

	}

	public override DbDataReader? DataReader {
		get { return _dr; }
		set { _dr = (NpgsqlDataReader)value!; }
	}

	public override void Dispose () {
		_dr?.Close ();
	}

	public override bool HasField (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		// dr.GetOrdinal no longer returns a -1 when the field name is missing. ppbbtthh
		foreach (Npgsql.Schema.NpgsqlDbColumn col in _dr.GetColumnSchema ()) {
			if (col.ColumnName == column) {
				return true;
			}
		}

		return false;
	}

	public override string GetString (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetString (ord);
	}
	public override string? GetString (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
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

	public override long GetLong (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetInt64 (ord);
	}
	public override long? GetLong (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetInt64 (ord);
	}

	public override long GetInt64 (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetInt64 (ord);
	}
	public override long? GetInt64 (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetInt64 (ord);
	}

	public override double GetDouble (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetDouble (ord);
	}
	public override double? GetDouble (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDouble (ord);
	}

	public override int GetInt (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetInt32 (ord);
	}
	public override int? GetInt (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetInt32 (ord);
	}

	public override short GetShort (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetInt16 (ord);
	}
	public override short? GetShort (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetInt16 (ord);
	}

	public override byte GetByte (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetByte (ord);
	}
	public override byte? GetByte (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetByte (ord);
	}

	public override byte[] GetByteArray (
		string column,
		int length
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		byte[] buf = new byte[length];

		var ord = GetOrdinal (column);
		_dr.GetBytes (ord, 0, buf, 0, length);
		return buf;
	}
	public override byte[]? GetByteArray (
		string column,
		int length,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		byte[] buf = new byte[length];
		_dr.GetBytes (ord, 0, buf, 0, length);
		return buf;
	}

	public override bool GetBool (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetBoolean (ord);
	}
	public override bool? GetBool (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetBoolean (ord);
	}

	public override DateTime GetDateTime (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		// breaking change with npgsql 6.x - returning datetimekind UTC where i was expecting local previously
		// i need to simply restore that for now but i need to address this better later
		// return dr.GetDateTime (dr.GetOrdinal (column));
		var ord = GetOrdinal (column);

		DateTime ret = _dr.GetDateTime (ord);

		if (ret.Kind == DateTimeKind.Utc) {
			ret = ret.ToLocalTime ();
		}

		return ret;
	}
	public override DateTime? GetDateTime (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		// breaking change with npgsql 6.x - returning datetimekind UTC where i was expecting local previously
		// i need to simply restore that for now but i need to address this better later
		// return dr.GetDateTime (dr.GetOrdinal (column));
		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		DateTime ret = _dr.GetDateTime (ord);

		if (ret.Kind == DateTimeKind.Utc) {
			ret = ret.ToLocalTime ();
		}

		return ret;
	}

	public override DateTimeOffset GetDateTimeOffset (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);

		return _dr.GetDateTime (ord);
	}
	public override DateTimeOffset? GetDateTimeOffset (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDateTime (ord);
	}

	public override TimeSpan GetTimeSpan (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetTimeSpan (ord);
	}
	public override TimeSpan? GetTimeSpan (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetTimeSpan (ord);
	}

	public override decimal GetDecimal (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetDecimal (ord);
	}
	public override decimal? GetDecimal (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDecimal (ord);
	}

	public override Guid GetGuid (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetGuid (ord);
	}
	public override Guid? GetGuid (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetGuid (ord);
	}

	//added by harv
	public override int[]? GetIntArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		return (int[])_dr.GetValue (GetOrdinal (column));
	}

	public override int[,]? GetIntArray2D (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		return (int[,])_dr.GetValue (GetOrdinal (column));
	}

	public override long[]? GetLongArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		return (long[])_dr.GetValue (GetOrdinal (column));
	}

	public override double[]? GetDoubleArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		return (double[])_dr.GetValue (GetOrdinal (column));
	}

	public override string[]? GetStringArray (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);

		// this may no longer be necessary
		if (_dr.GetValue (ord) == DBNull.Value) {
			string[]? val = null;
			return val;
		}

		return _dr.GetFieldValue<string[]> (ord);
	}

	public override string GetInet (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetValue (ord).ToString ()!;
	}
	public override string? GetInet (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetValue (ord).ToString ();
	}

	// 2016-06-28
	public override System.Net.IPAddress GetIpAddress (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return System.Net.IPAddress.Parse (_dr.GetValue (ord).ToString ()!);
	}
	public override System.Net.IPAddress? GetIpAddress (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		if (System.Net.IPAddress.TryParse (_dr.GetValue (ord).ToString (), out System.Net.IPAddress? addr)) {
			return addr;
		}

		return null;
	}

	// 2018-04-04
	public override System.Collections.BitArray GetBit (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return (System.Collections.BitArray)_dr.GetValue (ord);
	}
	public override System.Collections.BitArray? GetBit (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return (System.Collections.BitArray)_dr.GetValue (ord);
	}

	public override string GetJson (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetString (ord);
	}
	public override string? GetJson (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetString (ord);
	}

	public override string GetJsonb (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetString (ord);
	}
	public override string? GetJsonb (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetString (ord);
	}

	// 2020-08-24
	public override string? GetRegconfig (
		string column
	) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetValue (ord).ToString ();
	}

	private NpgsqlDataReader? _dr;
}
