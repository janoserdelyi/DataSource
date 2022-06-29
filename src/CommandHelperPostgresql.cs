using System;
using System.Data.Common;
using Npgsql;
using NpgsqlTypes;

namespace com.janoserdelyi.DataSource
{
	/// <summary>
	/// Summary description for CommandHelper.
	/// </summary>
	public class CommandHelperPostgresql : ICommandHelper, IDisposable
	{
		public CommandHelperPostgresql () {

		}

		public DbCommand Command {
			set { command = (NpgsqlCommand)value; }
		}

		//added 2008 01 09 janos erdelyi - to allow for IDisposable
		public void Dispose () {
			if (command != null) {
				command.Cancel ();
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
			//command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Int32, 4));
			//command.Parameters[param].Value = value;

			command.Parameters.AddWithValue (param, NpgsqlTypes.NpgsqlDbType.Integer, value);
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
			//command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Int64, 8));
			//command.Parameters[param].Value = value;

			command.Parameters.AddWithValue (param, NpgsqlTypes.NpgsqlDbType.Bigint, value);
		}

		public void Append (
			string param,
			double value
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Double));
			command.Parameters[param].Value = value;
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			short value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Int16, 4));
			command.Parameters[param].Value = value;
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			byte value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Byte));
			command.Parameters[param].Value = value;
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			bool value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Boolean, 1));
			command.Parameters[param].Value = value;
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			char value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.String, 1));
			command.Parameters[param].Value = value.ToString ();
		}

		public void Append (
			string param,
			System.Decimal value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Decimal));
			command.Parameters[param].Value = value;
		}

		public void AppendNumeric (
			string param,
			System.Decimal value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Numeric));
			command.Parameters[param].Value = value;
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		/// <param name="size">size</param>
		public void Append (
			string param,
			string value,
			int size
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.String, size));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		/// <param name="size">size</param>
		public void AppendChar (
			string param,
			string value,
			int size
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.String, size));
			if (value == null)
				command.Parameters[param].Value = DBNull.Value;
			else
				command.Parameters[param].Value = value;
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void AppendText (
			string param,
			string value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter () {
				ParameterName = param,
				Value = value,
				NpgsqlDbType = NpgsqlDbType.Text
			});
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			}
		}
		// 2011-06-13 janos.
		// text is different in postrgres than in MSSQL, so allow this without length definition
		public void Append (
			string param,
			string value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.String));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			DateTime value
		) {

			//command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.DateTime, 4));
			// npgsql 6.x made breaking changes to timezone with[out] time zone
			if (value.Kind != DateTimeKind.Utc) {
				value = value.ToUniversalTime ();
			}
			command.Parameters.Add (new Npgsql.NpgsqlParameter {
				ParameterName = param,
				Value = value,
				NpgsqlDbType = NpgsqlDbType.TimestampTz
			});
			command.Parameters[param].Value = value;
		}

		public void Append (
			string param,
			DateTimeOffset value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.DateTimeOffset));
			command.Parameters[param].Value = value;
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			Guid value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Guid));
			command.Parameters[param].Value = value;
		}

		//added by harv
		public void Append (
			string param,
			int[] value
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Integer));
			command.Parameters[param].Value = value;
		}
		public void Append (
			string param,
			long[] value
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Bigint));
			command.Parameters[param].Value = value;
		}
		public void Append (
			string param,
			double[] value
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Double));
			command.Parameters[param].Value = value;
		}
		public void Append (
			string param,
			DateTime[] value
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Timestamp));
			command.Parameters[param].Value = value;
		}
		public void Append (
			string param,
			string[] value
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Varchar));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value;
			}
		}
		public void Append (
			string param,
			int[,] value
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Array | NpgsqlDbType.Array | NpgsqlDbType.Integer));
			command.Parameters[param].Value = value;
		}

		// 2013-01-29
		[Obsolete ("use Append(string, IPAddress)")]
		public void AppendInet (
			string param,
			string value,
			int netmask = 32
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Inet));
			//command.Parameters[param].Value = new NpgsqlInet (value);
			command.Parameters[param].Value = new ValueTuple<System.Net.IPAddress, int> (System.Net.IPAddress.Parse (value), netmask);
		}

		public void Append (
			string param,
			System.Net.IPAddress value,
			int netmask = 32
		) {
			command.Parameters.Add (new NpgsqlParameter (param, NpgsqlDbType.Inet));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				// do i want to try to get the netmask from the broadcast?
				command.Parameters[param].Value = new ValueTuple<System.Net.IPAddress, int> (value, netmask);
			}
		}

		// 2014-10-05 added
		public void Append (
			string param,
			TimeSpan value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Time));
			command.Parameters[param].Value = value;
		}

		// 2016-04-02
		public void AppendJson (
			string param,
			string value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Json));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value;
			}
		}
		// 2018-07-25
		public void AppendJsonb (
			string param,
			string value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Jsonb));
			if (value == null) {
				command.Parameters[param].Value = DBNull.Value;
			} else {
				command.Parameters[param].Value = value;
			}
		}

		// 2018-04-04
		public void Append (
			string param,
			System.Collections.BitArray value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Varbit));
			command.Parameters[param].Value = value;
		}
		public void Append (
			string param,
			System.Collections.BitArray value,
			int size
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, NpgsqlDbType.Bit, size));
			command.Parameters[param].Value = value;
		}
		#endregion

		#region Nullable<t> Types

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			int? value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Int32, 4));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			long? value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Int64, 8));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			short? value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Int16, 4));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			byte? value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Byte));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			bool? value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Boolean, 1));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			char? value
		) {
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.String, 1));
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
			command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Decimal));
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		[Obsolete ("use Append(string, decimal?)")]
		public void Append (
			string param,
			System.Decimal? value,
			bool isMoney
		) {
			if (isMoney) { //TODO:investigate how this impacts postrgres. this is a carryover from MSSQL
				command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.SqlDbType.Money));
			} else {
				command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.Decimal));
			}
			if (value.HasValue) {
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			DateTime? value
		) {

			command.Parameters.Add (new Npgsql.NpgsqlParameter {
				ParameterName = param,
				NpgsqlDbType = NpgsqlDbType.TimestampTz
			});
			//command.Parameters[param].Value = value;

			//command.Parameters.Add (new Npgsql.NpgsqlParameter (param, System.Data.DbType.DateTime, 4));
			if (value.HasValue) {
				if (value.Value.Kind != DateTimeKind.Utc) {
					value = value.Value.ToUniversalTime ();
				}
				command.Parameters[param].Value = value.Value;
			} else {
				command.Parameters[param].Value = DBNull.Value;
			}
		}

		/// <summary></summary>
		/// <remarks>none</remarks>
		/// <returns>void</returns>
		/// <param name="param">Description for param</param>
		/// <param name="value">value</param>
		public void Append (
			string param,
			Guid? value
		) {
			throw new Exception ("Error, Guids are not yet supported in Npgsql.");
			/*
			command.Parameters.Add(new Npgsql.NpgsqlParameter(param, SqlDbType.UniqueIdentifier));
			command.Parameters[param].Value = value;
			*/
		}

		#endregion

		private NpgsqlCommand command;
	}
}
