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

		this.connection = connection;
		this.baseCommand = connection.GetDbCommand (commandText);
		this.commandHelper = connection.GetCommandHelper ();
		this.commandHelper.Command = this.baseCommand;
	}


	public override string CommandText {
		get {
			return this.baseCommand.CommandText;
		}
		set {
			this.baseCommand.CommandText = value;
		}
	}

	public override int CommandTimeout {
		get {
			return this.baseCommand.CommandTimeout;
		}
		set {
			this.baseCommand.CommandTimeout = value;
		}
	}

	public override CommandType CommandType {
		get {
			return this.baseCommand.CommandType;
		}
		set {
			this.baseCommand.CommandType = value;
		}
	}

	protected override DbConnection DbConnection {
		get {
			return this.connection;
		}
		set {
			this.baseCommand.Connection = value;
		}
	}

	public override int ExecuteNonQuery () {
		return this.baseCommand.ExecuteNonQuery ();
	}

	public override object? ExecuteScalar () {
		return this.baseCommand.ExecuteScalar ();
	}

	public override void Prepare () {
		this.baseCommand.Prepare ();
	}

	protected override DbDataReader ExecuteDbDataReader (CommandBehavior behavior) {
		throw new NotImplementedException ();
	}

	protected override DbParameter CreateDbParameter () {
		throw new NotImplementedException ();
	}

	public override void Cancel () {
		this.baseCommand.Cancel ();
	}

	public override UpdateRowSource UpdatedRowSource {
		get {
			return this.baseCommand.UpdatedRowSource;
		}
		set {
			this.baseCommand.UpdatedRowSource = value;
		}
	}

	public override bool DesignTimeVisible {
		get {
			return this.baseCommand.DesignTimeVisible;
		}
		set {
			this.baseCommand.DesignTimeVisible = value;
		}
	}

	protected override DbTransaction DbTransaction {
		get {
			throw new NotImplementedException ();
		}
		set {
			throw new NotImplementedException ();
		}
	}

	protected override DbParameterCollection DbParameterCollection {
		get {
			throw new NotImplementedException ();
		}
	}

	public new DbDataReader ExecuteReader () {

		//this will automatically generate a DataReaderHelper as well
		switch (this.connection.DatabaseType) {
			case DatabaseType.MSSQL:
				//throw new NotSupportedException ("MSSQL removed");
				this.dataReaderHelper = new DataReaderHelperMssql ();
				break;
			case DatabaseType.Postgresql:
				this.dataReaderHelper = new DataReaderHelperPostgresql ();
				break;
			case DatabaseType.MySql:
				this.dataReaderHelper = new DataReaderHelperMysql ();
				break;
			default:
				throw new Exception ("Error finding command helper. '" + this.connection.DatabaseType.ToString () + "' is not a currently supported database");
		}

		DbDataReader dr = this.baseCommand.ExecuteReader ();
		this.dataReaderHelper.DataReader = dr;
		return dr;
	}


	public DbCommand BaseCommand {
		get { return baseCommand; }
		set { baseCommand = value; }
	}

	public ICommandHelper CommandHelper {
		get { return commandHelper; }
	}
	public ICommandHelper CH {
		get { return commandHelper; }
	}

	public IDataReaderHelper DataReaderHelper {
		get { return dataReaderHelper; }
	}
	public IDataReaderHelper DRH {
		get { return dataReaderHelper; }
	}


	public new void Dispose () {
		if (dataReaderHelper != null) {
			dataReaderHelper.Dispose ();
		}
		if (baseCommand != null) {
			baseCommand.Dispose ();
		}
	}

	private Connection connection;
	private DbCommand baseCommand;
	private ICommandHelper commandHelper;
	private IDataReaderHelper dataReaderHelper;

}
