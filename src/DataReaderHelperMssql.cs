using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace com.janoserdelyi.DataSource
{
	public class DataReaderHelperMssql : IDataReaderHelper, IDisposable
	{
		public DataReaderHelperMssql () {

		}

		public DbDataReader DataReader {
			get { return dr; }
			set { dr = (SqlDataReader)value; }
		}

		public void Dispose () {
			if (dr != null) {
				dr.Close ();
				dr.Dispose ();
			}
		}

		public bool HasField (string column) {
			throw new NotImplementedException ();
		}

		#region Regular Types
		public int? GetInt (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt32 (ord);
		}

		public long? GetLong (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt64 (ord);
		}
		public long? GetInt64 (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetInt64 (ord);
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

		public decimal? GetMoney (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetSqlMoney (ord).ToDecimal ();
		}

		public decimal? GetDecimal (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDecimal (ord);
		}

		public double? GetDouble (string column) {
			throw new InvalidOperationException ("GetDouble is not programmed yet for MSSQL");
		}

		public string? GetString (string column) {
			int ord = dr.GetOrdinal (column);
			if (dr.IsDBNull (ord)) {
				return null;
			} else {
				return dr.GetString (ord);
			}
		}

		public DateTime? GetDateTime (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDateTime (ord);
		}

		public DateTimeOffset? GetDateTimeOffset (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetDateTimeOffset (ord);
		}

		public TimeSpan? GetTimeSpan (string column) {
			throw new InvalidOperationException ("GetTimeSpan is not programmed yet for MSSQL");
		}

		public Guid? GetGuid (string column) {
			int ord = dr.GetOrdinal (column);
			return dr.GetGuid (ord);
		}

		//2011-11-25 janos
		//NOTE: really here for postgresql. these would probably explode if used with mssql
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

		//2012-10-19. NOTE: there is no SQL native CIDR value in MSSQL. this is implemented with compatability for postgresql
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
			throw new InvalidOperationException ("GetBit is not yet implemented for MSSQL");
		}

		public string? GetJson (string column) {
			throw new InvalidOperationException ("GetJson is not yet implemented for MSSQL");
		}
		public string? GetJsonb (string column) {
			throw new InvalidOperationException ("GetJsonb is not yet implemented for MSSQL");
		}

		public string? GetRegconfig (string column) {
			throw new InvalidOperationException ("GetRegconfig is not yet implemented for MSSQL");
		}
		#endregion

		private SqlDataReader dr;
	}
}
