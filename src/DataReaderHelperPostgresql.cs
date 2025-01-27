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

	public double GetDouble ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetDouble (ord);
	}
	public double? GetDouble ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDouble (ord);
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

	public DateTime GetDateTime ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		// breaking change with npgsql 6.x - returning datetimekind UTC where i was expecting local previously
		// i need to simply restore that for now but i need to address this better later
		// return dr.GetDateTime (dr.GetOrdinal (column));
		var ord = dr.GetOrdinal (column);

		DateTime ret = dr.GetDateTime (ord);

		if (ret.Kind == DateTimeKind.Utc) {
			ret = ret.ToLocalTime ();
		}

		return ret;
	}
	public DateTime? GetDateTime ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		// breaking change with npgsql 6.x - returning datetimekind UTC where i was expecting local previously
		// i need to simply restore that for now but i need to address this better later
		// return dr.GetDateTime (dr.GetOrdinal (column));
		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		DateTime ret = dr.GetDateTime (ord);

		if (ret.Kind == DateTimeKind.Utc) {
			ret = ret.ToLocalTime ();
		}

		return ret;
	}

	public DateTimeOffset GetDateTimeOffset ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);

		return dr.GetDateTime (ord);
	}
	public DateTimeOffset? GetDateTimeOffset ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetDateTime (ord);
	}

	public TimeSpan GetTimeSpan ( string column ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetTimeSpan (ord);
	}
	public TimeSpan? GetTimeSpan ( string column, bool isNullable ) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetTimeSpan (ord);
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

		// this may no longer be necessary
		if (dr.GetValue (ord) == System.DBNull.Value) {
			string[]? val = null;
			return val;
		}

		return dr.GetFieldValue<string[]> (ord);
	}

	public string GetInet (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetValue (ord).ToString ()!;
	}
	public string? GetInet (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetValue (ord).ToString ();
	}

	// 2016-06-28 
	public System.Net.IPAddress GetIpAddress (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return System.Net.IPAddress.Parse (dr.GetValue (ord).ToString ()!);
	}
	public System.Net.IPAddress? GetIpAddress (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		if (System.Net.IPAddress.TryParse (dr.GetValue (ord).ToString (), out System.Net.IPAddress? addr)) {
			return addr;
		}
		return null;
	}

	// 2018-04-04
	public System.Collections.BitArray GetBit (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return (System.Collections.BitArray)dr.GetValue (ord);
	}
	public System.Collections.BitArray? GetBit (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return (System.Collections.BitArray)dr.GetValue (ord);
	}

	public string GetJson (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetString (ord);
	}
	public string? GetJson (
		string column,
		bool isNullable
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		if (dr.IsDBNull (ord)) {
			return null;
		}
		return dr.GetString (ord);
	}

	public string GetJsonb (
		string column
	) {
		ArgumentNullException.ThrowIfNull (dr);

		var ord = dr.GetOrdinal (column);
		return dr.GetString (ord);
	}
	public string? GetJsonb (
		string column,
		bool isNullable
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
		return dr.GetValue (ord).ToString ()!;
	}

	private NpgsqlDataReader? dr;
}
