using Microsoft.Data.SqlClient;

namespace com.janoserdelyi.DataSource;

public class DataReaderHelperMssql : IDataReaderHelper, IDisposable {
	public DataReaderHelperMssql () {

	}

	public DbDataReader? DataReader {
		get { return dr; }
		set { dr = (SqlDataReader)value!; }
	}

	public void Dispose () {
		if (dr != null) {
			dr.Close ();
			dr.Dispose ();
		}
	}

	public bool HasField ( string column ) {
		throw new NotImplementedException ();
	}

	public int GetInt ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetInt32 (ord);
	}
	public int? GetInt ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetInt32 (ord);
	}

	public long GetLong ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetInt64 (ord);
	}
	public long? GetLong ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetInt64 (ord);
	}
	public long GetInt64 ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetInt64 (ord);
	}
	public long? GetInt64 ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetInt64 (ord);
	}

	public short GetShort ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetInt16 (ord);
	}
	public short? GetShort ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetInt16 (ord);
	}

	public byte GetByte ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetByte (ord);
	}
	public byte? GetByte ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetByte (ord);
	}

	public bool GetBool ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetBoolean (ord);
	}
	public bool? GetBool ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetBoolean (ord);
	}

	public decimal GetDecimal ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetDecimal (ord);
	}
	public decimal? GetDecimal ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDecimal (ord);
	}

	public double GetDouble ( string column ) {
		throw new InvalidOperationException ("GetDouble is not programmed yet for MSSQL");
	}
	public double? GetDouble ( string column, bool isNullable ) {
		throw new InvalidOperationException ("GetDouble is not programmed yet for MSSQL");
	}

	public string GetString ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetString (ord);
	}
	public string? GetString ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetString (ord);
	}

	public DateTime GetDateTime ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetDateTime (ord);
	}
	public DateTime? GetDateTime ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDateTime (ord);
	}

	public DateTimeOffset GetDateTimeOffset ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetDateTimeOffset (ord);
	}
	public DateTimeOffset? GetDateTimeOffset ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDateTimeOffset (ord);
	}

	public TimeSpan GetTimeSpan ( string column ) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MSSQL");
	}
	public TimeSpan? GetTimeSpan ( string column, bool isNullable ) {
		throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MSSQL");
	}

	public Guid GetGuid ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetGuid (ord);
	}
	public Guid? GetGuid ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetGuid (ord);
	}

	//2011-11-25 janos
	//NOTE: really here for postgresql. these would probably explode if used with mssql
	public int[]? GetIntArray ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return (int[])dr.GetValue (ord);
	}
	public int[,]? GetIntArray2D ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return (int[,])dr.GetValue (ord);
	}

	public long[]? GetLongArray ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return (long[])dr.GetValue (ord);
	}
	public double[]? GetDoubleArray ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return (double[])dr.GetValue (ord);
	}
	public DateTime[]? GetDateTimeArray ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return (DateTime[])dr.GetValue (ord);
	}
	public string[]? GetStringArray ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return (string[])dr.GetValue (ord);
	}

	public string GetInet ( string column ) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MSSQL");
	}
	public string? GetInet ( string column, bool isNullable ) {
		throw new InvalidOperationException ("IpAddress is not yet a data type for MSSQL");
	}
	// 2016-06-28. 
	public System.Net.IPAddress GetIpAddress ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return System.Net.IPAddress.Parse (dr.GetValue (ord).ToString ()!);
	}
	public System.Net.IPAddress? GetIpAddress ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		if (!System.Net.IPAddress.TryParse (dr.GetValue (dr.GetOrdinal (column)).ToString (), out System.Net.IPAddress? addr)) {
			return null;
		}
		return addr;
	}

	// 2018-04-04
	public System.Collections.BitArray GetBit ( string column ) {
		throw new InvalidOperationException ("GetBit is not yet implemented for MSSQL");
	}
	public System.Collections.BitArray? GetBit ( string column, bool isNullable ) {
		throw new InvalidOperationException ("GetBit is not yet implemented for MSSQL");
	}

	public string GetJson ( string column ) {
		throw new InvalidOperationException ("GetJson is not yet implemented for MSSQL");
	}
	public string? GetJson ( string column, bool isNullable ) {
		throw new InvalidOperationException ("GetJson is not yet implemented for MSSQL");
	}
	public string GetJsonb ( string column ) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MSSQL");
	}
	public string? GetJsonb ( string column, bool isNullable ) {
		throw new InvalidOperationException ("GetJsonb is not yet implemented for MSSQL");
	}

	public string? GetRegconfig ( string column ) {
		throw new InvalidOperationException ("GetRegconfig is not yet implemented for MSSQL");
	}

	private SqlDataReader? dr;
}

