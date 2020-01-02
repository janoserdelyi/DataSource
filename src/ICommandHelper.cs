using System;
using System.Data;

namespace com.janoserdelyi.DataSource
{
	public interface ICommandHelper : IDisposable
	{
		/*
		IDbCommand Command {
			set;
		}
		*/
		System.Data.Common.DbCommand Command {
			set;
		}

		#region Regular Types

		//
		void Append (string param, int value);

		//
		void Append (string param, long value);

		//
		void Append (string param, short value);

		//
		void Append (string param, byte value);

		//
		void Append (string param, bool value);

		//
		void Append (string param, char value);

		// 2014-10-05 added
		void Append (string param, TimeSpan value);

		//
		void Append (string param, string value, int size);

		//this is a carry-over thing really meant for MSSQL
		void AppendChar (string param, string value, int size);

		//
		void AppendText (string param, string value);
		void Append (string param, string value); // 2018-07-27. finally. only implemented in postgresql and mysql

		// 
		void AppendNumeric (string param, decimal value);

		//
		void Append (string param, DateTime value);

		//
		void Append (string param, Guid value);

		// 2012-11-09. added for postgresql
		void Append (string param, string[] value);
		void Append (string param, int[] value);
		void Append (string param, int[,] value);
		void Append (string param, long[] value);
		void Append (string param, double[] value);
		void Append (string param, DateTime[] value);

		// 2013-01-29. added for postgresql
		void AppendInet (string param, string value, int netmask);
		// void AppendCidr(string param, string value); // as of this time, the version of npgsql i'm using does not support cidr
		// 2016-06-28 finally getting a little more strongly typed here
		void Append (string param, System.Net.IPAddress value, int netmask = 32);

		// 2016-04-02
		void AppendJson (string param, string value);
		// 2018-07-25
		void AppendJsonb (string param, string value);
		#endregion

		#region Nullable<t> Types

		//
		void Append (string param, int? value);

		//
		void Append (string param, long? value);

		//
		void Append (string param, short? value);

		//
		void Append (string param, byte? value);

		//
		void Append (string param, bool? value);

		//
		void Append (string param, char? value);

		//
		void Append (string param, DateTime? value);

		//
		void Append (string param, decimal? value);

		//
		void Append (string param, decimal? value, bool isMoney);

		//
		void Append (string param, Guid? value);

		void Append (string param, System.Collections.BitArray value);
		void Append (string param, System.Collections.BitArray value, int size);

		// 2018-06-19 i can't believe i went this long without this...
		//void AppendDefault(string param); // ok, so not quite implementing yet
		#endregion

		#region ExtTypes
		/*
		//
		void Append(string param, IntObj value);

		//
		void Append(string param, LongObj value);

		//
		void Append(string param, ShortObj value);

		//
		void Append(string param, CharObj value);

		//
		void Append(string param, ByteObj value);

		//
		void Append(string param, BoolObj value);

		//
		void Append(string param, DateTimeObj value);

		//
		void Append(string param, DecimalObj value);

		//
		void Append(string param, DecimalObj value, bool isMoney);

		//
		void Append(string param, GuidObj value);
		*/
		#endregion

	}
}
