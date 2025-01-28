using MySqlConnector;

namespace com.janoserdelyi.DataSource;

public class DataReaderHelperMysql : IDataReaderHelper, IDisposable
{
	public DataReaderHelperMysql () {

	}

	public DbDataReader? DataReader {
		get { return dr; }
		set { dr = (MySqlDataReader)value!; }
	}

	public void Dispose () {
		dr?.Close ();
	}

	public bool HasField (string column) {
		throw new NotImplementedException ();
	}

	public string GetString (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetString (ord);
	}
	public string? GetString (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetString (ord);
	}

	public long GetLong (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetInt64 (ord);
	}
	public long? GetLong (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetInt64 (ord);
	}
	public long GetInt64 (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetInt64 (ord);
	}
	public long? GetInt64 (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetInt64 (ord);
	}

	public double GetDouble (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetDouble (ord);
	}
	public double? GetDouble (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDouble (ord);
	}

	public int GetInt (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetInt32 (ord);
	}
	public int? GetInt (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetInt32 (ord);
	}

	public short GetShort (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetInt16 (ord);
	}
	public short? GetShort (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetInt16 (ord);
	}

	public byte GetByte (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetByte (ord);
	}
	public byte? GetByte (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetByte (ord);
	}

	public byte[] GetByteArray (
		string column,
		int length
	) {
		ArgumentNullException.ThrowIfNull (dr);

		byte[] buf = new byte[length];

		var ord = dr.GetOrdinal (column);
		dr.GetBytes (ord, 0, buf, 0, length);
		return buf;
	}
	public byte[]? GetByteArray (
		string column,
		int length,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		byte[] buf = new byte[length];
		dr.GetBytes (ord, 0, buf, 0, length);
		return buf;
	}

	public bool GetBool (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetBoolean (ord);
	}
	public bool? GetBool (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetBoolean (ord);
	}

	public DateTime GetDateTime (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetDateTime (ord);
	}
	public DateTime? GetDateTime (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDateTime (ord);
	}

	public DateTimeOffset GetDateTimeOffset (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetDateTimeOffset (ord);
	}
	public DateTimeOffset? GetDateTimeOffset (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDateTimeOffset (ord);
	}

	public TimeSpan GetTimeSpan (string column) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MySQL");
	}
	public TimeSpan? GetTimeSpan (string column, bool isNullable) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MySQL");
	}

	public decimal GetDecimal (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetDecimal (ord);
	}
	public decimal? GetDecimal (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDecimal (ord);
	}

	public Guid GetGuid (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetGuid (ord);
	}
	public Guid? GetGuid (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetGuid (ord);
	}

	//added by harv
	public int[]? GetIntArray (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return (int[])dr.GetValue (ord);
	}

	public int[,]? GetIntArray2D (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return (int[,])dr.GetValue (ord);
	}

	public long[]? GetLongArray (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return (long[])dr.GetValue (ord);
	}

	public double[]? GetDoubleArray (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return (double[])dr.GetValue (ord);
	}

	public DateTime[]? GetDateTimeArray (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return (DateTime[])dr.GetValue (ord);
	}

	public string[]? GetStringArray (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return (string[])dr.GetValue (ord);
	}

	public string GetInet (string column) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MySQL");
	}
	public string? GetInet (string column, bool isNullable) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MySQL");
	}

	// 2016-06-28.
	public System.Net.IPAddress GetIpAddress (string column) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return System.Net.IPAddress.Parse (dr.GetValue (ord).ToString ()!);
	}
	public System.Net.IPAddress? GetIpAddress (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (dr);

		if (!System.Net.IPAddress.TryParse (dr.GetValue (dr.GetOrdinal (column)).ToString (), out System.Net.IPAddress? addr)) {
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

	private MySqlDataReader? dr;
}
