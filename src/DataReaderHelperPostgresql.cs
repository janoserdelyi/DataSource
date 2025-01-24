using Npgsql;

namespace com.janoserdelyi.DataSource;

public class DataReaderHelperPostgresql : IDataReaderHelper, IDisposable {
	public DataReaderHelperPostgresql () {

	}

	public DbDataReader? DataReader {
		get { return dr; }
		set { dr = (NpgsqlDataReader)value!; }
	}

	public void Dispose () {
		dr?.Close ();
	}

	public bool HasField ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		// dr.GetOrdinal no longer returns a -1 when the field name is missing. ppbbtthh
		foreach (Npgsql.Schema.NpgsqlDbColumn col in dr.GetColumnSchema ()) {
			if (col.ColumnName == column) {
				return true;
			}
		}
		return false;
	}

	public string? GetString ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetString (ord);
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

	public long? GetLong ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetInt64 (dr.GetOrdinal (column));
	}

	public long? GetInt64 ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetInt64 (dr.GetOrdinal (column));
	}

	public double? GetDouble ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetDouble (dr.GetOrdinal (column));
	}

	public int? GetInt ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetInt32 (dr.GetOrdinal (column));
	}

	public short? GetShort ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetInt16 (dr.GetOrdinal (column));
	}

	public byte? GetByte ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetByte (dr.GetOrdinal (column));
	}

	public bool? GetBool ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetBoolean (dr.GetOrdinal (column));
	}

	public DateTime? GetDateTime ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		// breaking change with npgsql 6.x - returning datetimekind UTC where i was expecting local previously
		// i need to simply restore that for now but i need to address this better later
		// return dr.GetDateTime (dr.GetOrdinal (column));
		DateTime ret = dr.GetDateTime (dr.GetOrdinal (column));

		if (ret.Kind == DateTimeKind.Utc) {
			ret = ret.ToLocalTime ();
		}

		return ret;
	}

	public DateTimeOffset? GetDateTimeOffset ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetDateTime (dr.GetOrdinal (column));
	}

	public TimeSpan? GetTimeSpan ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return new TimeSpan (0, 0, 0);
		}
		return dr.GetTimeSpan (ord);
	}

	[Obsolete ("use GetDecimal(string)")]
	public decimal? GetMoney ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetDecimal (dr.GetOrdinal (column));
	}

	public decimal? GetDecimal ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetDecimal (dr.GetOrdinal (column));
	}

	public Guid? GetGuid ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetGuid (dr.GetOrdinal (column));
	}

	//added by harv
	public int[]? GetIntArray ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return (int[])dr.GetValue (dr.GetOrdinal (column));
	}

	public int[,]? GetIntArray2D ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return (int[,])dr.GetValue (dr.GetOrdinal (column));
	}

	public long[]? GetLongArray ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return (long[])dr.GetValue (dr.GetOrdinal (column));
	}

	public double[]? GetDoubleArray ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		return (double[])dr.GetValue (dr.GetOrdinal (column));
	}

	[Obsolete ("this has not been converted to deal with npgsql 6.x datetime handling changes. use at own risk")]
	public DateTime[]? GetDateTimeArray (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		// warning, these need to return Localtime but they will not since npgsql 6.x
		var rets = (DateTime[])dr.GetValue (dr.GetOrdinal (column));

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
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);

		// this may no longer be ncessary
		if (dr.GetValue (ord) == System.DBNull.Value) {
			string[]? val = null;
			return val;
		}

		return dr.GetFieldValue<string[]> (ord);
	}

	//2012-10-19
	[Obsolete ("use GetInet(string)")]
	public string? GetCIDR (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);
		return dr.GetValue (dr.GetOrdinal (column))?.ToString ();
	}

	public string? GetInet (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		return dr.GetValue (dr.GetOrdinal (column)).ToString ();
	}

	// 2016-06-28 
	public System.Net.IPAddress? GetIpAddress (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		if (System.Net.IPAddress.TryParse (dr.GetValue (dr.GetOrdinal (column)).ToString (), out System.Net.IPAddress? addr)) {
			return addr;
		}
		return null;
	}

	// 2018-04-04
	public System.Collections.BitArray? GetBit (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		return (System.Collections.BitArray)dr.GetValue (dr.GetOrdinal (column));
	}

	public string? GetJson (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetString (ord);
	}

	public string? GetJsonb (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetString (ord);
	}

	// 2020-08-24
	public string? GetRegconfig (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetValue (ord).ToString ();
	}

	private NpgsqlDataReader? dr;
}
