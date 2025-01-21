using System;
using System.Data.Common;

using MySqlConnector;
//using MySql.Data.MySqlClient;

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
		public string? GetString (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			} else {
				return dr.GetString (ord);
			}
		}

		public long? GetLong (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt64 (ord);
		}

		public long? GetInt64 (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt64 (ord);
		}

		public double? GetDouble (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDouble (ord);
		}

		public int? GetInt (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt32 (ord);
		}

		public short? GetShort (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt16 (ord);
		}

		public byte? GetByte (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetByte (ord);
		}

		public bool? GetBool (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetBoolean (ord);
		}

		public DateTime? GetDateTime (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDateTime (ord);
		}

		public DateTimeOffset? GetDateTimeOffset (string column) {
			throw new InvalidOperationException ("GetDateTimeOffset is not programmed yet for Mysql");
		}

		public TimeSpan? GetTimeSpan (string column) {
			throw new InvalidOperationException ("GetTimeSpan is not programmed yet for Mysql");
		}

		public decimal? GetMoney (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDecimal (ord);
		}
		public decimal? GetDecimal (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDecimal (ord);
		}

		public Guid? GetGuid (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetGuid (ord);
		}

		//added by harv
		public int[]? GetIntArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (int[])dr.GetValue (ord);
		}

		public int[,]? GetIntArray2D (string column) {
			int ord = dr.GetOrdinal (column);
			return (int[,])dr.GetValue (ord);
		}

		public long[]? GetLongArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (long[])dr.GetValue (ord);
		}

		public double[]? GetDoubleArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (double[])dr.GetValue (ord);
		}

		public DateTime[]? GetDateTimeArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (DateTime[])dr.GetValue (ord);
		}

		public string[]? GetStringArray (string column) {
			int ord = dr.GetOrdinal (column);
			return (string[])dr.GetValue (ord);
		}

		//2012-10-19
		public string? GetCIDR (string column) {
			return dr.GetValue (dr.GetOrdinal (column)).ToString ();
		}

		public string? GetInet (string column) {
			return dr.GetValue (dr.GetOrdinal (column)).ToString ();
		}

		// 2016-06-28.
		public System.Net.IPAddress? GetIpAddress (string column) {
			if (!System.Net.IPAddress.TryParse (dr.GetValue (dr.GetOrdinal (column)).ToString (), out System.Net.IPAddress? addr)) {
				return null;
			}
			return addr;
		}

		// 2018-04-04
		public System.Collections.BitArray? GetBit (string column) {
			throw new InvalidOperationException ("GetBit is not yet implemented for MySQL");
		}

		public string? GetJson (string column) {
			throw new InvalidOperationException ("GetJson is not yet implemented for MySQL");
		}
		public string? GetJsonb (string column) {
			throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
		}

		public string? GetRegconfig (string column) {
			throw new InvalidOperationException ("GetJsonb is not yet implemented for MySQL");
		}
		#endregion

		private MySqlDataReader dr;
	}
}
