using System.Data;
using System.Data.Common;
using System.Text;
using com.janoserdelyi.DataSource;

namespace UnitTests;

// https://xunit.net/docs/shared-context
public class DatabaseFixture : IDisposable
{
	public DatabaseFixture () {
		this.ConnectionManager = ConnectionManager.Instance;

		foreach (var connection in this.ConnectionManager.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				// establish a table to test against
				new Connect (POSTGRESQL_CONNECTION_NAME).Query ("drop table if exists public.test;").Go ();

				// create the table with all sorts of data types
				new Connect (POSTGRESQL_CONNECTION_NAME).Query (@"create table public.test (
id int primary key generated always as identity,
surrogate text not null unique,
name text not null,
big_number bigint not null,
small_number smallint not null,
single_byte binary not null,
bytes bytea not null,
charv text null,
charnv text null,
active bool not null default true,
created_dt timestamp with time zone not null default now()
				);").Go ();
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				// establish a table to test against
				new Connect (MSSQL_CONNECTION_NAME).Query ("drop table if exists dbo.test;").Go ();

				// create the table with all sorts of data types
				new Connect (MSSQL_CONNECTION_NAME).Query (@"create table dbo.test (
id int primary key identity,
surrogate varchar(20) not null unique,
name nvarchar(100) not null,
big_number bigint not null,
small_number smallint not null,
single_byte binary(1) not null,
bytes varbinary(1024) not null,
charv varchar(25) not null,
charnv nvarchar(25) not null,
active bit not null default 1,
created_dt datetimeoffset not null default sysdatetimeoffset()
				);").Go ();
			}
			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				// establish a table to test against
				new Connect (MYSQL_CONNECTION_NAME).Query ("drop table if exists test;").Go ();

				// create the table with all sorts of data types
				new Connect (MYSQL_CONNECTION_NAME).Query (@"create table test (
id int primary key auto_increment,
surrogate varchar(20) not null unique,
name varchar(100) not null,
big_number bigint not null,
small_number smallint not null,
single_byte tinyint unsigned not null,
bytes varbinary(1024) not null,
charv varchar(25) not null,
charnv varchar(25) not null,
active tinyint(1) not null default 1,
created_dt timestamp not null default current_timestamp
				);").Go ();
			}

			insertTestRecord (connection.Value.DatabaseType);
		}
	}

	public void Dispose () {
		foreach (var connection in this.ConnectionManager.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				// establish a table to test against
				new Connect (POSTGRESQL_CONNECTION_NAME).Query ("drop table if exists public.test;").Go ();
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				// establish a table to test against
				new Connect (MSSQL_CONNECTION_NAME).Query ("drop table if exists dbo.test;").Go ();
			}
			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				// establish a table to test against
				new Connect (MYSQL_CONNECTION_NAME).Query ("drop table if exists test;").Go ();
			}
		}
	}

	private TestRecord? insertTestRecord (
		DatabaseType dbtype
	) {
		var values = new TestRecord () {
			Surrogate = "foo",
			Name = "foo",
			BigNumber = (long)1,
			SmallNumber = (short)1,
			SingleByte = (byte)0x20, // empty char
			Bytes = Encoding.ASCII.GetBytes (new string (' ', 1024)), //empty chars
			Charv = "varchar",
			Charnv = "nvarchar"
		};

		switch (dbtype) {
			case DatabaseType.Postgresql:
				return new Connect (POSTGRESQL_CONNECTION_NAME)
					.Query (@"insert into public.test (
	surrogate,
	name,
	big_number,
	small_number,
	single_byte,
	bytes
)
values (
	:surrogate,
	:name,
	:big_number,
	:small_number,
	:single_byte,
	:bytes
)
returning *;")
					.Append ("surrogate", values.Surrogate)
					.Append ("name", values.Name)
					.Append ("big_number", values.BigNumber)
					.Append ("small_number", values.SmallNumber)
					.Append ("single_byte", values.SingleByte)
					.Append ("bytes", values.Bytes)
					.Go<TestRecord?> (TestRecord.getTestRecord);
			case DatabaseType.MSSQL:
				return new Connect (MSSQL_CONNECTION_NAME)
					.Query (@"insert into dbo.test (
	surrogate,
	name,
	big_number,
	small_number,
	single_byte,
	bytes,
	charv,
	charnv
)
output inserted.*
values (
	@surrogate,
	@name,
	@big_number,
	@small_number,
	@single_byte,
	@bytes,
	@charv,
	@charnv
);")
					.Append ("surrogate", values.Surrogate)
					.Append ("name", values.Name)
					.Append ("big_number", values.BigNumber)
					.Append ("small_number", values.SmallNumber)
					.Append ("single_byte", values.SingleByte)
					.Append ("bytes", values.Bytes)
					.AppendVarchar ("charv", values.Charv)
					.AppendNvarchar ("charnv", values.Charnv)
					.Go<TestRecord?> (TestRecord.getTestRecord);
			case DatabaseType.MySql:
				new Connect (MYSQL_CONNECTION_NAME)
					.Query (@"insert into test (
	surrogate,
	name,
	big_number,
	small_number,
	single_byte,
	bytes,
	charv,
	charnv
)
values (
	?surrogate,
	?name,
	?big_number,
	?small_number,
	?single_byte,
	?bytes,
	?charv,
	?charnv
);")
					.Append ("surrogate", values.Surrogate)
					.Append ("name", values.Name)
					.Append ("big_number", values.BigNumber)
					.Append ("small_number", values.SmallNumber)
					.Append ("single_byte", values.SingleByte)
					.Append ("bytes", values.Bytes)
					.AppendVarchar ("charv", values.Charv)
					.AppendVarchar ("charnv", values.Charnv)
					.Go ();

				// MySQL doesn't have RETURNING, so we need a separate query to get the inserted record
				return new Connect (MYSQL_CONNECTION_NAME)
					.Query ("select * from test where surrogate = ?surrogate;")
					.Append ("surrogate", values.Surrogate)
					.Go<TestRecord?> (TestRecord.getTestRecord);
		}

		return null;
	}

	public ConnectionManager ConnectionManager { get; set; }
	public readonly string POSTGRESQL_CONNECTION_NAME = "TestPostgresql";
	public readonly string MSSQL_CONNECTION_NAME = "TestMssql";
	public readonly string MYSQL_CONNECTION_NAME = "TestMariadb";
}

public class Database : IClassFixture<DatabaseFixture>
{

	readonly DatabaseFixture fixture;
	readonly ConnectionManager cm;
	readonly string POSTGRESQL_CONNECTION_NAME;
	readonly string MSSQL_CONNECTION_NAME;
	readonly string MYSQL_CONNECTION_NAME;

	public Database (
		DatabaseFixture fixture
	) {
		this.fixture = fixture;

		cm = fixture.ConnectionManager;
		POSTGRESQL_CONNECTION_NAME = this.fixture.POSTGRESQL_CONNECTION_NAME;
		MSSQL_CONNECTION_NAME = this.fixture.MSSQL_CONNECTION_NAME;
		MYSQL_CONNECTION_NAME = this.fixture.MYSQL_CONNECTION_NAME;
	}

	[Fact]
	public void Get_TestRecord_ExpectNone () {
		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (POSTGRESQL_CONNECTION_NAME).Query ("select * from public.test where name = 'bar';").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.Null (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME).Query ("select * from dbo.test where name = 'bar';").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.Null (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (MYSQL_CONNECTION_NAME).Query ("select * from test where name = 'bar';").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.Null (record);
			}
		}
	}

	// [Fact]
	// public void Insert_TestRecord_ExpectInsertion () {
	// 	foreach (var connection in cm.Connections) {
	// 		if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
	// 			var record = insertTestRecord (connection.Value.DatabaseType);

	// 			Assert.NotNull (record);
	// 		}
	// 		if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
	// 			var record = insertTestRecord (connection.Value.DatabaseType);

	// 			Assert.NotNull (record);
	// 		}
	// 	}
	// }

	// let's test every data type
	/*
	int
	long
	short
	byte
	bool
	char
	TimeSpan
	string
	 - plain
	 - text
	 - varchar
	 - nvarchar
	decimal
	DateTime
	DateTimeOffset
	.. and null versions
	*/
	[Fact]
	public void Append_Int_ExpectSuccess () {

		int id = 1;

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where id = :id;")
					.Append ("id", id)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (MYSQL_CONNECTION_NAME)
					.Query ("select * from test where id = ?id;")
					.Append ("id", id)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Append_Long_ExpectSuccess () {

		long bignum = 1;

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where big_number = :big_number;")
					.Append ("big_number", bignum)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where big_number = @big_number;")
					.Append ("big_number", bignum)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Append_Short_ExpectSuccess () {

		short smallnum = 1;

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where small_number = :small_number;")
					.Append ("small_number", smallnum)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where small_number = @small_number;")
					.Append ("small_number", smallnum)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	// byte

	[Fact]
	public void Append_Bool_ExpectSuccess () {

		bool active = true;

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where active = :active;")
					.Append ("active", active)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where active = @active;")
					.Append ("active", active)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Append_PlainString_ExpectSuccess () {
		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where name = :name;")
					.Append ("name", "foo")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where name = @name;")
					.Append ("name", "foo")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (MYSQL_CONNECTION_NAME)
					.Query ("select * from test where name = ?name;")
					.Append ("name", "foo")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Append_Varchar_ExpectSuccess () {
		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where charv = @charv;")
					.AppendVarchar ("charv", "varchar")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Append_Nvarchar_ExpectSuccess () {
		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where charnv = @charnv;")
					.AppendNvarchar ("charnv", "nvarchar")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Call_ScalarFunction_ExpectTrue () {
		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select prefs.IsSendableStatus(@EmailAddress) as foo;")
					.AppendNvarchar ("EmailAddress", "jerdelyi@costar.com")
					.Go<bool> ((cmd) => {
						using (var dr = cmd.ExecuteReader ()) {
							ArgumentNullException.ThrowIfNull (cmd.DRH);

							if (dr.Read ()) {
								return cmd.DRH.GetBool ("foo");
							}
							throw new NullReferenceException ("error calling prefs.IsSendableStatus with value jerdelyi@costar.com");
						}
					});

				Assert.True (result);
			}
		}
	}

	[Fact]
	public void Append_Udt_ExpectSuccess () {

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {

				var dt = new DataTable ();
				dt.Columns.Add ("id", typeof (int));
				dt.Rows.Add (1);
				dt.Rows.Add (2);
				dt.Rows.Add (3);

				var result = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("select id from @ids;")
					.Append ("ids", dt, "dbo.IdArrayType")
					.Go<List<int>> ((cmd) => {
						using (var dr = cmd.ExecuteReader ()) {
							ArgumentNullException.ThrowIfNull (cmd.DRH);

							var rets = new List<int> ();
							while (dr.Read ()) {
								rets.Add (cmd.DRH.GetInt ("id"));
							}
							return rets;
						}
					});

			}
		}
	}
}
