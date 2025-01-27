namespace com.janoserdelyi.DataSource;

public interface IDataReaderHelper : IDisposable {
	//2007 12 19 janos erdelyi. added getter
	DbDataReader? DataReader { get; set; }

	// 2018-10-22 i want to see if a column exists to select in the first place
	// this may be a mistake, but we'll see
	bool HasField ( string column );

	//
	int GetInt ( string column );
	int? GetInt ( string column, bool isNullable );

	double GetDouble ( string column );
	double? GetDouble ( string column, bool isNullable );

	//
	long GetLong ( string column );
	long? GetLong ( string column, bool isNullable );
	long GetInt64 ( string column );
	long? GetInt64 ( string column, bool isNullable );

	//
	short GetShort ( string column );
	short? GetShort ( string column, bool isNullable );

	//
	byte GetByte ( string column );
	byte? GetByte ( string column, bool isNullable );

	//
	bool GetBool ( string column );
	bool? GetBool ( string column, bool isNullable );

	//
	decimal GetDecimal ( string column );
	decimal? GetDecimal ( string column, bool isNullable );

	//
	string GetString ( string column );
	string? GetString ( string column, bool isNullable );

	//
	DateTime GetDateTime ( string column );
	DateTime? GetDateTime ( string column, bool isNullable );
	DateTimeOffset GetDateTimeOffset ( string column );
	DateTimeOffset? GetDateTimeOffset ( string column, bool isNullable );

	// added 2014-10-05 janos. i was hoping i would never need this gross crap. NYC data storing times, floating in space, like "09:00"
	TimeSpan GetTimeSpan ( string column );
	TimeSpan? GetTimeSpan ( string column, bool isNullable );

	//
	Guid GetGuid ( string column );
	Guid? GetGuid ( string column, bool isNullable );

	int[]? GetIntArray ( string column );
	int[,]? GetIntArray2D ( string column );

	long[]? GetLongArray ( string column );
	double[]? GetDoubleArray ( string column );
	DateTime[]? GetDateTimeArray ( string column );
	string[]? GetStringArray ( string column );

	string GetInet ( string column );
	string? GetInet ( string column, bool isNullable );
	// 2016-06-28. temp hack. i hope
	System.Net.IPAddress GetIpAddress ( string column );
	System.Net.IPAddress? GetIpAddress ( string column, bool isNullable );

	System.Collections.BitArray GetBit ( string column );
	System.Collections.BitArray? GetBit ( string column, bool isNullable );

	// 2018-07-26 finally a possible need for this
	string GetJson ( string column );
	string? GetJson ( string column, bool isNullable );
	string GetJsonb ( string column );
	string? GetJsonb ( string column, bool isNullable );

	string? GetRegconfig ( string column );
}

