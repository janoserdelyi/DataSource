using System;
using System.Text;
using System.Data;
using System.Data.Common;

using MySqlConnector;
//using MySql;
//using MySql.Data;
//using MySql.Data.MySqlClient;
//using MySql.Data.Types;

// using com.reactivesoftware.ExtTypes;

namespace com.janoserdelyi.DataSource
{
	/// <summary>
	/// Summary description for CommandHelper.
	/// </summary>
	public class CommandHelperMysql : ICommandHelper, IDisposable
	{
		///<summary>Constructor for CommandHelper</summary>
		public CommandHelperMysql () {

		}
		/*
		public IDbCommand Command {
			set { command = (NpgsqlCommand)value; }
		}
		*/
		public DbCommand Command {
			set { command = (MySqlCommand)value; }
		}

		//added 2008 01 09 janos erdelyi - to allow for IDisposable
		public void Dispose () {
			if (command != null) {
				command.Cancel ();
				//command.Dispose();
			}
		}

		#region Regular Types

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			int value
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int32, 4));
			command.Parameters[param].Value = value;
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			long value
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int64, 8));
			command.Parameters[param].Value = value;
		}

		/*
		public void Append (
			string param, 
			double value
		) {
			command.Parameters.Add (new MySqlParameter(param, System.Data.DbType.Double));
			command.Parameters[param].Value = value;
		}
		*/
		public void Append (
			string param,
			double value
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Double));
			command.Parameters[param].Value = value;
		}

		public void Append (
			string param,
			short value
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int16, 4));
			command.Parameters[param].Value = value;
		}

		public void Append (
			string param,
			byte value
		) {
			command.Parameters.Add (new MySqlParameter (param, System.Data.DbType.Byte));
			command.Parameters[param].Value = value;
		}

		/// // HOLY CRAP it's the year 2015 and no boolean in mysql? 
		/// // i'm going to abuse a bit for this i suppose. or i may just throw an unsupported exception
		public void Append (
			string param,
			bool value
		) {
			//command.Parameters.Add(new MySqlParameter(param, MySqlDbType., 1));
			//command.Parameters[param].Value = value;
			throw new InvalidOperationException ("Mysql does not have a boolean. Please abuse some other data type");
		}

		public void Append (
			string param,
			char value
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String, 1));
			command.Parameters[param].Value = value.ToString ();
		}

		public void Append (
			string param,
			System.Decimal value
		) {
			command.Parameters.Add (new MySqlParameter (param, System.Data.DbType.Decimal));
			command.Parameters[param].Value = value;
		}

		public void AppendNumeric (
			string param,
			System.Decimal value
		) {
			throw new InvalidOperationException ("Not implemented. Append a decimal instead.");
		}

		public void Append (
			string param,
			string value,
			int size
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String, size));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value;
			}
		}

		public void AppendChar (
			string param,
			string value,
			int size
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String, size));
			if (value == null)
				command.Parameters[param].Value = DBNull.Value;
			else
				command.Parameters[param].Value = value;
		}

		public void AppendText (
			string param,
			string value
		) {
			command.Parameters.Add (new MySqlParameter (param, value));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value;
			}
		}
		// 2011-06-13 janos.
		// text is different in postrgres than in MSSQL, so allow this without length definition
		public void Append (
			string param,
			string val
		) {
			command.Parameters.Add (new MySqlParameter (param, System.Data.DbType.String));
			if (val == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = val;
			}
		}

		public void Append (
			string param,
			DateTime value
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.DateTime, 4));
			command.Parameters[param].Value = value;
		}
		public void Append (
			string param,
			DateTimeOffset value
		) {
			throw new InvalidOperationException ("Not yet implemented.");
		}

		public void Append (
			string param,
			Guid value
		) {
			command.Parameters.Add (new MySqlParameter (param, System.Data.DbType.Guid));
			command.Parameters[param].Value = value;
		}

		//added by harv
		public void Append (string param, int[] value) {
			throw new InvalidOperationException ("Not yet implemented.");
		}
		public void Append (string param, long[] value) {
			throw new InvalidOperationException ("Not yet implemented.");
		}
		public void Append (string param, double[] value) {
			throw new InvalidOperationException ("Not yet implemented.");
		}
		public void Append (string param, DateTime[] value) {
			throw new InvalidOperationException ("Not yet implemented.");
		}
		public void Append (string param, string[] value) {
			throw new InvalidOperationException ("Not yet implemented.");
		}
		public void Append (string param, int[,] value) {
			throw new InvalidOperationException ("Not yet implemented.");
		}

		// 2013-01-29
		public void AppendInet (string param, string value, int netmask = 32) {
			throw new InvalidOperationException ("Not yet implemented.");
		}

		public void Append (string param, System.Net.IPAddress value, int netmask = 32) {
			AppendInet (param, value.ToString ());
		}

		// 2014-10-05 added
		public void Append (
			string param,
			TimeSpan value
		) {
			throw new InvalidOperationException ("Not yet implemented.");
		}

		// 2016-04-02
		public void AppendJson (
			string param,
			string value
		) {
			throw new InvalidOperationException ("Not yet implemented.");
		}
		// 2018-07-25
		public void AppendJsonb (
			string param,
			string value
		) {
			throw new InvalidOperationException ("Not yet implemented.");
		}

		// 2018-04-04
		public void Append (
			string param,
			System.Collections.BitArray value
		) {
			throw new InvalidOperationException ("Not yet implemented.");
		}
		public void Append (
			string param,
			System.Collections.BitArray value,
			int size
		) {
			throw new InvalidOperationException ("Not yet implemented.");
		}
		#endregion

		#region Nullable<t> Types
		public void Append (
			string param,
			int? value
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int32, 4));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		public void Append (
			string param,
			long? value
		) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int64, 8));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		public void Append (string param, short? value) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.Int16, 4));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		public void Append (string param, byte? value) {
			command.Parameters.Add (new MySqlParameter (param, System.Data.DbType.Byte));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		public void Append (string param, bool? value) {
			/*
			command.Parameters.Add(new MySqlParameter(param, System.Data.DbType.Boolean, 1));
			if (value.HasValue){
				command.Parameters[param].Value = value.Value;
			}else{
				command.Parameters[param].Value = DBNull.Value;
			}
			*/
			throw new InvalidOperationException ("Mysql does not have a boolean. Please abuse some other data type");
		}

		public void Append (string param, char? value) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.String, 1));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		public void Append (
			string param,
			System.Decimal? value
		) {
			command.Parameters.Add (new MySqlParameter (param, System.Data.DbType.Decimal));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		public void Append (string param, System.Decimal? value, bool isMoney) {
			if (isMoney) { //TODO:investigate how this impacts postrgres. this is a carryover from MSSQL
				command.Parameters.Add (new MySqlParameter (param, System.Data.SqlDbType.Money));
			} else {
				command.Parameters.Add (new MySqlParameter (param, System.Data.DbType.Decimal));
			}
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		public void Append (string param, DateTime? value) {
			command.Parameters.Add (new MySqlParameter (param, MySqlDbType.DateTime, 4));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		public void Append (string param, Guid? value) {
			throw new Exception ("Error, Not yet implemented.");
			/*
			command.Parameters.Add(new MySqlParameter(param, SqlDbType.UniqueIdentifier));
			command.Parameters[param].Value = value;
			*/
		}
		#endregion

		#region ExtTypes
		/*
		[Obsolete("Convert to native nullable types")]
		public void Append (string param, IntObj value) {
			command.Parameters.Add(new MySqlParameter(param, MySqlDbType.Int32, 4));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value.Value;
			}
		}

		[Obsolete("Convert to native nullable types")]
		public void Append (
			string param, 
			LongObj value
		) {
			command.Parameters.Add(new MySqlParameter(param, MySqlDbType.Int64, 8));
			if (value == null) { command.Parameters[param].Value = DBNull.Value; }
			else { command.Parameters[param].Value = value.Value; }
		}

		[Obsolete("Convert to native nullable types")]
		public void Append (string param, DoubleObj value) {
			command.Parameters.Add(new MySqlParameter(param, System.Data.DbType.Double));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value.Value;
			}
		}

		[Obsolete("Convert to native nullable types")]
		public void Append(string param, ShortObj value) {
			command.Parameters.Add(new MySqlParameter(param, MySqlDbType.Int16, 2));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value.Value;
			}
		}

		[Obsolete("Convert to native nullable types")]
		public void Append(string param, CharObj value) {
			command.Parameters.Add(new MySqlParameter(param, MySqlDbType.String, 1));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value.Value;
			}
		}

		[Obsolete("Convert to native nullable types")]
		public void Append(string param, ByteObj value) {
			command.Parameters.Add(new MySqlParameter(param, System.Data.DbType.Byte));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value.Value;
			}
		}

		[Obsolete("Convert to native nullable types")]
		public void Append(string param, BoolObj value) {
			throw new InvalidOperationException("Mysql does not have a boolean. Please abuse some other data type");
		}

		[Obsolete("Convert to native nullable types")]
		public void Append(string param, DateTimeObj value) {
			command.Parameters.Add(new MySqlParameter(param, MySqlDbType.DateTime, 4));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value.Value;
			}
		}

		[Obsolete("Convert to native nullable types")]
		public void Append(string param, DecimalObj value) {
			command.Parameters.Add(new MySqlParameter(param, System.Data.DbType.Decimal));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value.Value;
			}
		}

		[Obsolete("Convert to native nullable types")]
		public void Append(string param, DecimalObj value, bool isMoney) {
			if (isMoney) { //TODO:investigate how this impacts postrgres. this is a carryover from MSSQL
				command.Parameters.Add(new MySqlParameter(param, System.Data.SqlDbType.Money));
			} else {
				command.Parameters.Add(new MySqlParameter(param, System.Data.DbType.Decimal));
			}
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value.Value;
			}
		}

		[Obsolete("Convert to native nullable types")]
		public void Append(string param, GuidObj value) {
			throw new InvalidOperationException("Not yet supported.");
		}
		*/
		#endregion

		private MySqlCommand command;
	}
}
