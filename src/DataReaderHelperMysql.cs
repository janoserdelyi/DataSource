using MySqlConnector;

namespace com.janoserdelyi.DataSource;

public class DataReaderHelperMysql : IDataReaderHelper, IDisposable
{
	public DataReaderHelperMysql () {

	}

	public DbDataReader? DataReader {
		get { return _dr; }
		set { _dr = (MySqlDataReader)value!; }
	}

	public void Dispose () {
		_dr?.Close ();
	}

	public bool HasField (string column) {
		throw new NotImplementedException ();
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

		var ord = _dr.GetOrdinal (column);
		return _dr.GetDateTime (ord);
	}
	public DateTime? GetDateTime (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDateTime (ord);
	}

	public DateTimeOffset GetDateTimeOffset (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return _dr.GetDateTimeOffset (ord);
	}
	public DateTimeOffset? GetDateTimeOffset (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDateTimeOffset (ord);
	}

	public TimeSpan GetTimeSpan (string column) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MySQL");
	}
	public TimeSpan? GetTimeSpan (string column, bool isNullable) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MySQL");
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

		var ord = _dr.GetOrdinal (column);
		return (int[])_dr.GetValue (ord);
	}

	public int[,]? GetIntArray2D (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return (int[,])_dr.GetValue (ord);
	}

	public long[]? GetLongArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return (long[])_dr.GetValue (ord);
	}

	public double[]? GetDoubleArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return (double[])_dr.GetValue (ord);
	}

	public DateTime[]? GetDateTimeArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return (DateTime[])_dr.GetValue (ord);
	}

	public string[]? GetStringArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return (string[])_dr.GetValue (ord);
	}

	public string GetInet (string column) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MySQL");
	}
	public string? GetInet (string column, bool isNullable) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MySQL");
	}

	// 2016-06-28.
	public System.Net.IPAddress GetIpAddress (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		return System.Net.IPAddress.Parse (_dr.GetValue (ord).ToString ()!);
	}
	public System.Net.IPAddress? GetIpAddress (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		if (!System.Net.IPAddress.TryParse (_dr.GetValue (_dr.GetOrdinal (column)).ToString (), out System.Net.IPAddress? addr)) {
			return null;
		}

		return addr;
	}

	// 2018-04-04
	public System.Collections.BitArray GetBit (string column) {
		throw new InvalidOperationException ("GetBit is not yet implemented for MySQL");
	}
	public System.Collections.BitArray? GetBit (string column, bool isNullable) {
		throw new InvalidOperationException ("GetBit is not yet implemented for MySQL");
	}

	public string GetJson (string column) {
		throw new InvalidOperationException ("GetJson is not yet implemented for MySQL");
	}
	public string? GetJson (string column, bool isNullable) {
		throw new InvalidOperationException ("GetJson is not yet implemented for MySQL");
	}
	public string GetJsonb (string column) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
	}
	public string? GetJsonb (string column, bool isNullable) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
	}

	public string? GetRegconfig (string column) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
	}

	private MySqlDataReader? _dr;
}
