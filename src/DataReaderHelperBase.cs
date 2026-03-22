namespace com.janoserdelyi.DataSource;

public abstract class DataReaderHelperBase : IDataReaderHelper, IDisposable
{
	private readonly Dictionary<string, int> _ordinalCache = new (StringComparer.OrdinalIgnoreCase);

	protected int GetOrdinal (string column) {
		var dr = DataReader;
		ArgumentNullException.ThrowIfNull (dr);

		if (!_ordinalCache.TryGetValue (column, out int ord)) {
			ord = dr.GetOrdinal (column);
			_ordinalCache[column] = ord;
		}

		return ord;
	}

	public abstract DbDataReader? DataReader { get; set; }
	public abstract void Dispose ();
	public abstract bool HasField (string column);
	public abstract int GetInt (string column);
	public abstract int? GetInt (string column, bool isNullable);
	public abstract double GetDouble (string column);
	public abstract double? GetDouble (string column, bool isNullable);
	public abstract long GetLong (string column);
	public abstract long? GetLong (string column, bool isNullable);
	public abstract long GetInt64 (string column);
	public abstract long? GetInt64 (string column, bool isNullable);
	public abstract short GetShort (string column);
	public abstract short? GetShort (string column, bool isNullable);
	public abstract byte GetByte (string column);
	public abstract byte? GetByte (string column, bool isNullable);
	public abstract byte[] GetByteArray (string column, int length);
	public abstract byte[]? GetByteArray (string column, int length, bool isNullable);
	public abstract bool GetBool (string column);
	public abstract bool? GetBool (string column, bool isNullable);
	public abstract decimal GetDecimal (string column);
	public abstract decimal? GetDecimal (string column, bool isNullable);
	public abstract string GetString (string column);
	public abstract string? GetString (string column, bool isNullable);
	public abstract DateTime GetDateTime (string column);
	public abstract DateTime? GetDateTime (string column, bool isNullable);
	public abstract DateTimeOffset GetDateTimeOffset (string column);
	public abstract DateTimeOffset? GetDateTimeOffset (string column, bool isNullable);
	public abstract TimeSpan GetTimeSpan (string column);
	public abstract TimeSpan? GetTimeSpan (string column, bool isNullable);
	public abstract Guid GetGuid (string column);
	public abstract Guid? GetGuid (string column, bool isNullable);
	public abstract int[]? GetIntArray (string column);
	public abstract int[,]? GetIntArray2D (string column);
	public abstract long[]? GetLongArray (string column);
	public abstract double[]? GetDoubleArray (string column);
	public abstract string[]? GetStringArray (string column);
	public abstract string GetInet (string column);
	public abstract string? GetInet (string column, bool isNullable);
	public abstract System.Net.IPAddress GetIpAddress (string column);
	public abstract System.Net.IPAddress? GetIpAddress (string column, bool isNullable);
	public abstract System.Collections.BitArray GetBit (string column);
	public abstract System.Collections.BitArray? GetBit (string column, bool isNullable);
	public abstract string GetJson (string column);
	public abstract string? GetJson (string column, bool isNullable);
	public abstract string GetJsonb (string column);
	public abstract string? GetJsonb (string column, bool isNullable);
	public abstract string? GetRegconfig (string column);
}
