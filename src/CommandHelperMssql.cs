using System.Text;
using Microsoft.Data.SqlClient;

namespace com.janoserdelyi.DataSource;

public class CommandHelperMssql : ICommandHelper, IDisposable {

	public CommandHelperMssql () {
		command = new SqlCommand ();
	}

	public DbCommand Command {
		set { command = (SqlCommand)value; }
	}

	//added 2008 01 09 janos erdelyi - to allow for IDisposable
	public void Dispose () {
		if (command != null) {
			command.Cancel ();
			//command.Dispose();
		}
	}

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		int value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Int, 4));
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.BigInt, 8));
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.SmallInt, 4));
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.TinyInt));
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Bit, 1));
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Char, 1));
		command.Parameters[param].Value = value.ToString ();
	}

	public void Append (
		string param,
		System.Decimal value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Decimal));
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
		string? value,
		int size
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.VarChar, size));
		if (value == null) {
			command.Parameters[param].Value = DBNull.Value;
		} else {
			command.Parameters[param].Value = value;
		}
	}

	public void AppendNvarchar (
		string param,
		string value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.NVarChar)); // leaving size out. documents suggest it is inferred from the actual data sent
		if (value == null) {
			command.Parameters[param].Value = DBNull.Value;
		} else {
			command.Parameters[param].Value = value;
		}
	}

	public void AppendVarchar (
		string param,
		string value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.VarChar)); // leaving size out. documents suggest it is inferred from the actual data sent
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Char, size));
		if (value == null)
			command.Parameters[param].Value = DBNull.Value;
		else
			command.Parameters[param].Value = value;
	}

	public void Append (
		string param,
		string value
	) {
		throw new NotImplementedException ();
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Text));
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
	public void Append (
		string param,
		DateTime value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.DateTime, 4));
		command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		DateTimeOffset value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.DateTimeOffset));
		command.Parameters[param].Value = value;
	}
	public void Append (
		string param,
		DateTimeOffset? value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.DateTimeOffset));
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.UniqueIdentifier));
		command.Parameters[param].Value = value;
	}


	public void Append ( string param, int[] value ) {
		throw new NotImplementedException ();
	}
	public void Append ( string param, long[] value ) {
		throw new NotImplementedException ();
	}
	public void Append ( string param, double[] value ) {
		throw new NotImplementedException ();
	}
	public void Append ( string param, DateTime[] value ) {
		throw new NotImplementedException ();
	}
	public void Append ( string param, string[] value ) {
		throw new NotImplementedException ();
	}
	public void Append ( string param, int[,] value ) {
		throw new NotImplementedException ();
	}

	public void AppendNumeric ( string param, decimal value ) {
		throw new NotImplementedException ();
	}

	// 2013-01-29
	public void AppendInet ( string param, string value, int netmask = 32 ) {
		throw new NotImplementedException ();
	}
	public void Append ( string param, System.Net.IPAddress value, int netmask = 32 ) {
		AppendInet (param, value.ToString ());
	}

	public void Append (
		string param,
		TimeSpan value
	) {
		throw new NotImplementedException ();
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

	/// <summary></summary>
	/// <remarks>none</remarks>
	/// <returns>void</returns>
	/// <param name="param">Description for param</param>
	/// <param name="value">value</param>
	public void Append (
		string param,
		int? value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Int, 4));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.BigInt, 8));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.SmallInt, 4));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.TinyInt));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Bit, 1));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Char, 1));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
	}

	public void Append (
		string param,
		System.Decimal? value
	) {
		command.Parameters.Add (new SqlParameter (param, SqlDbType.Decimal));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
	}

	public void Append (
		string param,
		System.Decimal? value,
		bool isMoney
	) {
		if (isMoney) {
			command.Parameters.Add (new SqlParameter (param, SqlDbType.Money));
		} else {
			command.Parameters.Add (new SqlParameter (param, SqlDbType.Decimal));
		}
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.DateTime, 4));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
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
		command.Parameters.Add (new SqlParameter (param, SqlDbType.UniqueIdentifier));
		if (value.HasValue)
			command.Parameters[param].Value = value.Value;
		else
			command.Parameters[param].Value = DBNull.Value;
	}

	public void Return (
		string param,
		SqlDbType dbtype
	) {
		var p = new SqlParameter (param, dbtype);
		p.Direction = ParameterDirection.ReturnValue;
		command.Parameters.Add (p);
	}

	public void Return (
		string param,
		SqlDbType dbtype,
		int size
	) {
		var p = new SqlParameter (param, dbtype, size);
		p.Direction = ParameterDirection.ReturnValue;
		command.Parameters.Add (p);
	}

	private string Join ( char[] ary ) {
		StringBuilder sb = new StringBuilder (8);
		for (int i = 0; i < ary.Length; i++) {
			sb.Append (ary[i]);
		}
		return sb.ToString ();
	}

	private SqlCommand command;
}
