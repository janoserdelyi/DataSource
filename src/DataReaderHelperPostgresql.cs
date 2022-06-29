using System;
using System.Data.Common;
using Npgsql;

namespace com.janoserdelyi.DataSource
{
	public class DataReaderHelperPostgresql : IDataReaderHelper, IDisposable
	{
		public DataReaderHelperPostgresql () {

		}

		public DbDataReader DataReader {
			get { return dr; }
			set { dr = (NpgsqlDataReader)value; }
		}

		public void Dispose () {
			if (dr != null) {
				dr.Close ();
				//dr.Dispose();
			}
		}

		public bool HasField (string column) {
			// dr.GetOrdinal no longer returns a -1 when the field name is missing. ppbbtthh
			foreach (Npgsql.Schema.NpgsqlDbColumn col in dr.GetColumnSchema ()) {
				if (col.ColumnName == column) {
					return true;
				}
			}
			return false;
		}

		#region Regular Types
		public string GetString (string column) {
			int ord = dr.GetOrdinal (column);
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
			int ord = dr.GetOrdinal (column);

			switch (typeof (T)) {
				case string s:

					break;
			}

		}
		*/

		// 2022-06-28
		// i'm not convinced i like this. like at all
		// fi this is the trash i'm thinking about, i'll just leave it in the client app
		//public System.Numerics.BigInteger GetBigInteger (string column) {
		//	return System.Numerics.BigInteger.Parse (dr.GetString (dr.GetOrdinal (column)));
		//}

		public long GetLong (string column) {
			return dr.GetInt64 (dr.GetOrdinal (column));
		}

		public long GetInt64 (string column) {
			return dr.GetInt64 (dr.GetOrdinal (column));
		}

		public double GetDouble (string column) {
			return dr.GetDouble (dr.GetOrdinal (column));
		}

		public int GetInt (string column) {
			return dr.GetInt32 (dr.GetOrdinal (column));
		}

		public short GetShort (string column) {
			return dr.GetInt16 (dr.GetOrdinal (column));
		}

		public byte GetByte (string column) {
			return dr.GetByte (dr.GetOrdinal (column));
		}

		public bool GetBool (string column) {
			return dr.GetBoolean (dr.GetOrdinal (column));
		}

		public DateTime GetDateTime (string column) {
			// breaking change with npgsql 6.x - returning datetimekind UTC where i was expecting local previously
			// i need to simply restore that for now but i need to address this better later
			// return dr.GetDateTime (dr.GetOrdinal (column));
			DateTime ret = dr.GetDateTime (dr.GetOrdinal (column));

			if (ret.Kind == DateTimeKind.Utc) {
				ret = ret.ToLocalTime ();
			}

			return ret;
		}

		public TimeSpan GetTimeSpan (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return new TimeSpan (0, 0, 0);
			}
			return dr.GetTimeSpan (ord);
		}

		[Obsolete ("use GetDecimal(string)")]
		public decimal GetMoney (string column) {
			return dr.GetDecimal (dr.GetOrdinal (column));
		}

		public decimal GetDecimal (string column) {
			return dr.GetDecimal (dr.GetOrdinal (column));
		}

		public Guid GetGuid (string column) {
			return dr.GetGuid (dr.GetOrdinal (column));
		}

		//added by harv
		public int[] GetIntArray (string column) {
			return (int[])dr.GetValue (dr.GetOrdinal (column));
		}

		public int[,] GetIntArray2D (string column) {
			return (int[,])dr.GetValue (dr.GetOrdinal (column));
		}

		public long[] GetLongArray (string column) {
			return (long[])dr.GetValue (dr.GetOrdinal (column));
		}

		public double[] GetDoubleArray (string column) {
			return (double[])dr.GetValue (dr.GetOrdinal (column));
		}

		public DateTime[] GetDateTimeArray (string column) {
			// warning, these need to return Localtime but they will not since npgsql 6.x
			return (DateTime[])dr.GetValue (dr.GetOrdinal (column));
		}

		public string[] GetStringArray (string column) {
			int ord = dr.GetOrdinal (column);
			// this may no longer be ncessary
			if (dr.GetValue (ord) == System.DBNull.Value) {
				string[] val = null;
				return val;
			}
			return dr.GetFieldValue<string[]> (ord);
		}

		//2012-10-19
		[Obsolete ("use GetInet(string)")]
		public string GetCIDR (string column) {
			return dr.GetValue (dr.GetOrdinal (column)).ToString ();
		}

		public string GetInet (string column) {
			return dr.GetValue (dr.GetOrdinal (column)).ToString ();
		}

		// 2016-06-28 
		public System.Net.IPAddress GetIpAddress (string column) {
			if (System.Net.IPAddress.TryParse (dr.GetValue (dr.GetOrdinal (column)).ToString (), out System.Net.IPAddress addr)) {
				return addr;
			}
			return null;
		}

		// 2018-04-04
		public System.Collections.BitArray GetBit (string column) {
			return (System.Collections.BitArray)dr.GetValue (dr.GetOrdinal (column));
		}

		public string GetJson (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetString (ord);
		}

		public string GetJsonb (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetString (ord);
		}

		// 2020-08-24
		public string GetRegconfig (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetValue (ord).ToString ();
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
			//return dr.GetDateTime (ord);

			DateTime ret = dr.GetDateTime (ord);

			if (ret.Kind == DateTimeKind.Utc) {
				ret = ret.ToLocalTime ();
			}

			return ret;
		}

		public TimeSpan? GetTimeSpanNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetTimeSpan (ord);
		}

		public Guid? GetGuidNullable (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			}
			return dr.GetGuid (ord);
		}
		#endregion

		private NpgsqlDataReader dr;
	}
}
