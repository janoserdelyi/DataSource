using MySqlConnector;

namespace com.janoserdelyi.DataSource;

public class DataReaderHelperMysql : DataReaderHelperBase
{
	public DataReaderHelperMysql () {

	}

	public override DbDataReader? DataReader {
		get { return _dr; }
		set { _dr = (MySqlDataReader)value!; }
	}

	public override void Dispose () {
		_dr?.Close ();
	}

	public override bool HasField (string column) {
		throw new NotImplementedException ();
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

		var ord = GetOrdinal (column);
		return _dr.GetDateTime (ord);
	}
	public override DateTime? GetDateTime (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDateTime (ord);
	}

	public override DateTimeOffset GetDateTimeOffset (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return _dr.GetDateTimeOffset (ord);
	}
	public override DateTimeOffset? GetDateTimeOffset (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		if (_dr.IsDBNull (ord)) {
			return null;
		}

		return _dr.GetDateTimeOffset (ord);
	}

	public override TimeSpan GetTimeSpan (string column) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MySQL");
	}
	public override TimeSpan? GetTimeSpan (string column, bool isNullable) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MySQL");
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

		var ord = GetOrdinal (column);
		return (int[])_dr.GetValue (ord);
	}

	public override int[,]? GetIntArray2D (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return (int[,])_dr.GetValue (ord);
	}

	public override long[]? GetLongArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return (long[])_dr.GetValue (ord);
	}

	public override double[]? GetDoubleArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return (double[])_dr.GetValue (ord);
	}

	public override string[]? GetStringArray (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return (string[])_dr.GetValue (ord);
	}

	public override string GetInet (string column) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MySQL");
	}
	public override string? GetInet (string column, bool isNullable) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MySQL");
	}

	// 2016-06-28.
	public override System.Net.IPAddress GetIpAddress (string column) {
		ArgumentNullException.ThrowIfNull (_dr);

		var ord = GetOrdinal (column);
		return System.Net.IPAddress.Parse (_dr.GetValue (ord).ToString ()!);
	}
	public override System.Net.IPAddress? GetIpAddress (string column, bool isNullable) {
		ArgumentNullException.ThrowIfNull (_dr);

		if (!System.Net.IPAddress.TryParse (_dr.GetValue (GetOrdinal (column)).ToString (), out System.Net.IPAddress? addr)) {
			return null;
		}

		return addr;
	}

	// 2018-04-04
	public override System.Collections.BitArray GetBit (string column) {
		throw new InvalidOperationException ("GetBit is not yet implemented for MySQL");
	}
	public override System.Collections.BitArray? GetBit (string column, bool isNullable) {
		throw new InvalidOperationException ("GetBit is not yet implemented for MySQL");
	}

	public override string GetJson (string column) {
		throw new InvalidOperationException ("GetJson is not yet implemented for MySQL");
	}
	public override string? GetJson (string column, bool isNullable) {
		throw new InvalidOperationException ("GetJson is not yet implemented for MySQL");
	}
	public override string GetJsonb (string column) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
	}
	public override string? GetJsonb (string column, bool isNullable) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
	}

	public override string? GetRegconfig (string column) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
	}

	private MySqlDataReader? _dr;
}
