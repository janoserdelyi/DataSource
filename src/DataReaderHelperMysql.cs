using System;
using System.Data.Common;

// using com.reactivesoftware.ExtTypes;
using MySql.Data.MySqlClient;

namespace com.janoserdelyi.DataSource
{
	public class DataReaderHelperMysql : IDataReaderHelper, IDisposable
	{
		public DataReaderHelperMysql () {

		}

		public DbDataReader DataReader {
			get { return dr; }
			set { dr = (MySqlDataReader)value; }
		}

		public void Dispose () {
			if (dr != null) {
				dr.Close ();
				//dr.Dispose();
			}
		}

		public bool HasField (string column) {
			throw new NotImplementedException ();
		}

		#region Regular Types
		public string GetString (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			} else {
				return dr.GetString (ord);
			}
		}

		public long GetLong (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt64 (ord);
		}

		public long GetInt64 (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt64 (ord);
		}

		public double GetDouble (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDouble (ord);
		}

		public int GetInt (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt32 (ord);
		}

		public short GetShort (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt16 (ord);
		}

		public byte GetByte (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetByte (ord);
		}

		public bool GetBool (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetBoolean (ord);
		}

		public DateTime GetDateTime (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDateTime (ord);
		}

		public TimeSpan GetTimeSpan (string column) {
			throw new InvalidOperationException ("GetTimeSpan is not programmed yet for Mysql");
		}

		public decimal GetMoney (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDecimal (ord);
		}
		public decimal GetDecimal (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDecimal (ord);
		}

		public Guid GetGuid (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetGuid (ord);
		}

		//added by harv
		public int[] GetIntArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (int[])dr.GetValue (ord);
		}

		public int[,] GetIntArray2D (string column) {
			int ord = dr.GetOrdinal (column);
			return (int[,])dr.GetValue (ord);
		}

		public long[] GetLongArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (long[])dr.GetValue (ord);
		}

		public double[] GetDoubleArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (double[])dr.GetValue (ord);
		}

		public DateTime[] GetDateTimeArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (DateTime[])dr.GetValue (ord);
		}

		public string[] GetStringArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (string[])dr.GetValue (ord);
		}

		//2012-10-19
		public string GetCIDR (string column) {
			return dr.GetValue (dr.GetOrdinal (column)).ToString ();
		}

		public string GetInet (string column) {
			return dr.GetValue (dr.GetOrdinal (column)).ToString ();
		}

		// 2016-06-28.
		public System.Net.IPAddress GetIpAddress (string column) {
			System.Net.IPAddress addr;
			if (!System.Net.IPAddress.TryParse (dr.GetValue (dr.GetOrdinal (column)).ToString (), out addr)) {
				return null;
			}
			return addr;
		}

		// 2018-04-04
		public System.Collections.BitArray GetBit (string column) {
			throw new InvalidOperationException ("GetBit is not yet implemented for MySQL");
		}

		public string GetJson (string column) {
			throw new InvalidOperationException ("GetJson is not yet implemented for MySQL");
		}
		public string GetJsonb (string column) {
			throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
		}
		#endregion

		#region Nullable<t> Types
		public int? GetIntNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetInt32 (ord);
		}

		public long? GetLongNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetInt64 (ord);
		}

		public short? GetShortNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetInt16 (ord);
		}

		public byte? GetByteNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetByte (ord);
		}

		public bool? GetBoolNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetBoolean (ord);
		}

		public decimal? GetMoneyNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetDecimal (ord);
		}

		public decimal? GetDecimalNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetDecimal (ord);
		}

		public DateTime? GetDateTimeNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetDateTime (ord);
		}

		public TimeSpan? GetTimeSpanNullable (string column) {
			throw new InvalidOperationException ("GetTimeSpanNullable is not programmed yet for Mysql");
		}

		public Guid? GetGuidNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetGuid (ord);
		}
		#endregion

		#region ExtTypes
		/*
		[Obsolete("Use native nullable types")]
		public BoolObj GetBoolObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) return null;
			else return new BoolObj(dr.GetBoolean(ord));
		}

		[Obsolete("Use native nullable types")]
		public LongObj GetLongObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) return null;
			else return new LongObj(dr.GetInt64(ord));
		}

		[Obsolete("Use native nullable types")]
		public LongObj GetInt64Obj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) return null;
			else return new LongObj(dr.GetInt64(ord));
		}

		[Obsolete("Use native nullable types")]
		public DoubleObj GetDoubleObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) return null;
			else return new DoubleObj(dr.GetDouble(ord));
		}

		[Obsolete("Use native nullable types")]
		public IntObj GetIntObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) {
				return null;
			} else {
				return new IntObj(dr.GetInt32(ord));
			}
		}

		[Obsolete("Use native nullable types")]
		public ShortObj GetShortObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) return null;
			else return new ShortObj(dr.GetInt16(ord));
		}

		[Obsolete("Use native nullable types")]
		public ByteObj GetByteObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) return null;
			else return new ByteObj(dr.GetByte(ord));
		}

		[Obsolete("Use native nullable types")]
		public CharObj GetCharObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) return null;
			else return new CharObj(dr.GetString(ord)[0]);
		}

		[Obsolete("Use native nullable types")]
		public DateTimeObj GetDateTimeObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord))
				return null;
			return new DateTimeObj(dr.GetDateTime(ord));
		}

		[Obsolete("Use native nullable types")]
		public DecimalObj GetDecimalObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord))
				return null;

			if (dr.GetDataTypeName(ord).ToLower() == "money") {
				//return new DecimalObj(dr.GetSqlMoney(ord).ToDecimal());
			}
			return new DecimalObj(dr.GetDecimal(ord));
		}

		[Obsolete("Use native nullable types")]
		public DecimalObj GetMoneyObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord)) {
				return null;
			}
			//return new DecimalObj(dr.GetSqlMoney(ord).ToDecimal());
			return new DecimalObj(dr.GetDecimal(ord));
		}

		[Obsolete("Use native nullable types")]
		public GuidObj GetGuidObj(string column) {
			int ord = dr.GetOrdinal(column);
			if (dr.IsDBNull(ord))
				return null;
			return new GuidObj(dr.GetGuid(ord));
		}
		*/
		#endregion

		private MySqlDataReader dr;
	}
}
