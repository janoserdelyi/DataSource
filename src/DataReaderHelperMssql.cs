using Microsoft.Data.SqlClient;

namespace com.janoserdelyi.DataSource;

public class DataReaderHelperMssql : IDataReaderHelper, IDisposable
{
	public DataReaderHelperMssql () {

	}

	public DbDataReader? DataReader {
		get { return _dr; }
		set { _dr = (SqlDataReader)value!; }
	}

	public void Dispose () {
		if (_dr != null) {
			_dr.Close ();
			_dr.Dispose ();
		}
	}

	public bool HasField (string column) {
		throw new NotImplementedException ();
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

		var len = _dr.GetBytes (ord, 0, null, 0, 1);

		if (len < 1) {
			Exception? oops = null;
			throw new ArgumentOutOfRangeException ("no byte returned", oops);
		}

		if (len > 1) {
			Exception? oops = null;
			throw new ArgumentOutOfRangeException ($"only one byte was expected, but {len} are present", oops);
		}

		// this appears to be potentially returning more than one byte. so get a byte array and return the first element. using dr.GetByte(ord)

		byte[] buf = new byte[1];
		_dr.GetBytes (ord, 0, buf, 0, 1);
		return buf[0];
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

		var ord = _dr.GetOrdinal (column);

		// var len = _dr.GetBytes (ord, 0, null, 0, length);

		byte[] buf = new byte[length];

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

	public double GetDouble (string column) {
		throw new InvalidOperationException ("GetDouble is not programmed yet for MSSQL");
	}
	public double? GetDouble (string column, bool isNullable) {
		throw new InvalidOperationException ("GetDouble is not programmed yet for MSSQL");
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
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MSSQL");
	}
	public TimeSpan? GetTimeSpan (string column, bool isNullable) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MSSQL");
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

	//2011-11-25 janos
	//NOTE: really here for postgresql. these would probably explode if used with mssql
	public int[]? GetIntArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return (int[])_dr.GetValue (ord);
	}
	public int[,]? GetIntArray2D (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return (int[,])_dr.GetValue (ord);
	}

	public long[]? GetLongArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return (long[])_dr.GetValue (ord);
	}
	public double[]? GetDoubleArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return (double[])_dr.GetValue (ord);
	}
	public DateTime[]? GetDateTimeArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return (DateTime[])_dr.GetValue (ord);
	}
	public string[]? GetStringArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = _dr.GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return (string[])_dr.GetValue (ord);
	}

	public string GetInet (string column) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MSSQL");
	}
	public string? GetInet (string column, bool isNullable) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MSSQL");
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
		throw new InvalidOperationException ("GetBit is not yet implemented for MSSQL");
	}
	public System.Collections.BitArray? GetBit (string column, bool isNullable) {
		throw new InvalidOperationException ("GetBit is not yet implemented for MSSQL");
	}

	public string GetJson (string column) {
		throw new InvalidOperationException ("GetJson is not yet implemented for MSSQL");
	}
	public string? GetJson (string column, bool isNullable) {
		throw new InvalidOperationException ("GetJson is not yet implemented for MSSQL");
	}
	public string GetJsonb (string column) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MSSQL");
	}
	public string? GetJsonb (string column, bool isNullable) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MSSQL");
	}

	public string? GetRegconfig (string column) {
		throw new InvalidOperationException ("GetRegconfig is not yet implemented for MSSQL");
	}

	private SqlDataReader? _dr;
}

