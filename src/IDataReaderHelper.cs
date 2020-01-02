using System;
using System.Data;
using System.Data.Common;

namespace com.janoserdelyi.DataSource
{
	public interface IDataReaderHelper : IDisposable
	{
		//2007 12 19 janos erdelyi. added getter
		DbDataReader DataReader { get; set; }

		// 2018-10-22 i want to see if a column exists to select in the first place
		// this may be a mistake, but we'll see
		bool HasField (string column);

		//
		int GetInt (string column);

		double GetDouble (string column);

		//
		long GetLong (string column);
		long GetInt64 (string column);

		//
		short GetShort (string column);

		//
		byte GetByte (string column);

		//
		bool GetBool (string column);

		//
		decimal GetMoney (string column);
		decimal GetDecimal (string column);
		//
		string GetString (string column);

		//
		DateTime GetDateTime (string column);

		// added 2014-10-05 janos. i was hoping i would never need this gross crap. NYC data storing times, floating in space, like "09:00"
		TimeSpan GetTimeSpan (string column);

		//
		Guid GetGuid (string column);

		int[] GetIntArray (string column);
		int[,] GetIntArray2D (string column);

		long[] GetLongArray (string column);
		double[] GetDoubleArray (string column);
		DateTime[] GetDateTimeArray (string column);
		string[] GetStringArray (string column);

		//2012-10-19. wow i haven't looked at this code in a long time
		//in postgresql i have a CIDR column. i will just extract it as a string for consumption in C# since there is no native type
		string GetCIDR (string column);

		string GetInet (string column);
		// 2016-06-28. temp hack. i hope
		System.Net.IPAddress GetIpAddress (string column);

		//
		int? GetIntNullable (string column);

		//
		long? GetLongNullable (string column);

		//
		short? GetShortNullable (string column);

		//
		byte? GetByteNullable (string column);

		//
		bool? GetBoolNullable (string column);

		//
		decimal? GetMoneyNullable (string column);

		decimal? GetDecimalNullable (string column);

		//
		DateTime? GetDateTimeNullable (string column);

		//
		Guid? GetGuidNullable (string column);

		System.Collections.BitArray GetBit (string column);

		// 2018-07-26 finally a possibly need for this
		string GetJson (string column);
		string GetJsonb (string column);
	}
}
