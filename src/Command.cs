using System.Diagnostics.CodeAnalysis;

namespace com.janoserdelyi.DataSource;

public class Command : DbCommand, IDisposable
{

	public Command (
		Connection connection,
		string commandText
	) {
		if (string.IsNullOrEmpty (commandText)) {
			throw new System.ArgumentNullException ("commandText");
		}

		_connection = connection;
		BaseCommand = connection.getDbCommand (commandText);
		CH = connection.GetCommandHelper ();
		CH.Command = BaseCommand;
	}

	[AllowNull]
	public override string CommandText {
		get {
			ArgumentNullException.ThrowIfNull (BaseCommand);
			ArgumentNullException.ThrowIfNull (BaseCommand.CommandText);
			return BaseCommand.CommandText;
		}
		set {
			BaseCommand.CommandText = value ?? "";
		}
	}

	public override int CommandTimeout {
		get {
			return BaseCommand.CommandTimeout;
		}
		set {
			BaseCommand.CommandTimeout = value;
		}
	}

	public override CommandType CommandType {
		get {
			return BaseCommand.CommandType;
		}
		set {
			BaseCommand.CommandType = value;
		}
	}

	protected override DbConnection? DbConnection {
		get {
			return _connection;
		}
		set {
			BaseCommand.Connection = value;
		}
	}

	public override int ExecuteNonQuery () {
		return BaseCommand.ExecuteNonQuery ();
	}

	public override object? ExecuteScalar () {
		return BaseCommand.ExecuteScalar ();
	}

	public override void Prepare () {
		BaseCommand.Prepare ();
	}

	protected override DbDataReader ExecuteDbDataReader (CommandBehavior behavior) {
		throw new NotImplementedException ();
	}

	protected override DbParameter CreateDbParameter () {
		throw new NotImplementedException ();
	}

	public override void Cancel () {
		BaseCommand.Cancel ();
	}

	public override UpdateRowSource UpdatedRowSource {
		get {
			return BaseCommand.UpdatedRowSource;
		}
		set {
			BaseCommand.UpdatedRowSource = value;
		}
	}

	public override bool DesignTimeVisible {
		get {
			return BaseCommand.DesignTimeVisible;
		}
		set {
			BaseCommand.DesignTimeVisible = value;
		}
	}

	protected override DbTransaction? DbTransaction {
		get => throw new NotImplementedException ();
		set => throw new NotImplementedException ();
	}

	protected override DbParameterCollection DbParameterCollection {
		get {
			throw new NotImplementedException ();
		}
	}

	public new DbDataReader ExecuteReader () {

		if (_connection.State != ConnectionState.Open) {
			_connection.Open ();
		}

		//this will automatically generate a DataReaderHelper as well
		DRH = _connection.DatabaseType switch {
			DatabaseType.MSSQL => new DataReaderHelperMssql (),//throw new NotSupportedException ("MSSQL removed");
			DatabaseType.Postgresql => new DataReaderHelperPostgresql (),
			DatabaseType.MySql => new DataReaderHelperMysql (),
			_ => throw new Exception ("Error finding command helper. '" + _connection.DatabaseType.ToString () + "' is not a currently supported database"),
		};
		DbDataReader dr = BaseCommand.ExecuteReader ();
		DRH.DataReader = dr;
		return dr;
	}

	public DbCommand BaseCommand { get; set; }

	public ICommandHelper CommandHelper {
		get { return CH; }
	}
	public ICommandHelper CH { get; }

	public IDataReaderHelper? DataReaderHelper {
		get { return DRH; }
	}
	public IDataReaderHelper? DRH { get; private set; }

	public new void Dispose () {
		DRH?.Dispose ();
		BaseCommand?.Dispose ();
	}

	private readonly Connection _connection;
}
