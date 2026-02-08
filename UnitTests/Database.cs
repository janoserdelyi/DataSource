using System.Data;
// using System.Data.Common;
using System.Text;
using com.janoserdelyi.DataSource;

namespace UnitTests;

// https://xunit.net/docs/shared-context
public class DatabaseFixture : IDisposable
{
	public DatabaseFixture () {
		ConnectionManager = ConnectionManager.Instance;

		// PostgreSQL
		// establish a table to test against
		_ = new Connect (POSTGRESQL_CONNECTION_NAME).Query ("drop table if exists public.test;").Go ();

		// create the table with all sorts of data types
		_ = new Connect (POSTGRESQL_CONNECTION_NAME).Query (@"create table public.test (
	id int primary key generated always as identity,
	surrogate text not null unique,
	name text not null,
	big_number bigint not null,
	small_number smallint not null,
	bytes bytea not null,
	charv text not null,
	charnv text not null,
	active bool not null default true,
	created_dt timestamp with time zone not null default now()
);").Go ();

		_ = insertTestRecord (DatabaseType.Postgresql);

		// MSSQL
		// establish a table to test against
		// 		_ = new Connect (MSSQL_CONNECTION_NAME).Query ("drop table if exists dbo.test;").Go ();

		// 		// create the table with all sorts of data types
		// 		_ = new Connect (MSSQL_CONNECTION_NAME).Query (@"create table dbo.test (
		// 	id int primary key identity,
		// 	surrogate varchar(20) not null unique,
		// 	name nvarchar(100) not null,
		// 	big_number bigint not null,
		// 	small_number smallint not null,
		// 	bytes varbinary(1024) not null,
		// 	charv varchar(25) not null,
		// 	charnv nvarchar(25) not null,
		// 	active bit not null default 1,
		// 	created_dt datetimeoffset not null default sysdatetimeoffset()
		// );").Go ();

		// 		_ = insertTestRecord (DatabaseType.MSSQL);

		// MariaDB
		// establish a table to test against
		// 		_ = new Connect (MYSQL_CONNECTION_NAME).Query ("drop table if exists test;").Go ();

		// 		// create the table with all sorts of data types
		// 		_ = new Connect (MYSQL_CONNECTION_NAME).Query (@"create table test (
		// 	id int primary key auto_increment,
		// 	surrogate varchar(20) not null unique,
		// 	name varchar(100) not null,
		// 	big_number bigint not null,
		// 	small_number smallint not null,
		// 	bytes varbinary(1024) not null,
		// 	charv varchar(25) not null,
		// 	charnv varchar(25) not null,
		// 	active tinyint(1) not null default 1,
		// 	created_dt timestamp not null default current_timestamp
		// );").Go ();

		// 		_ = insertTestRecord (DatabaseType.MySql);
	}

	public void Dispose () {
		foreach (var connection in ConnectionManager.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				// establish a table to test against
				_ = new Connect (POSTGRESQL_CONNECTION_NAME).Query ("drop table if exists public.test;").Go ();
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				// establish a table to test against
				_ = new Connect (MSSQL_CONNECTION_NAME).Query ("drop table if exists dbo.test;").Go ();
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				// establish a table to test against
				_ = new Connect (MYSQL_CONNECTION_NAME).Query ("drop table if exists test;").Go ();
			}
		}
	}

	private TestRecord? insertTestRecord (
		DatabaseType dbtype
	) {
		var values = new TestRecord () {
			Surrogate = "foo",
			Name = "foo",
			BigNumber = 1,
			SmallNumber = 1,
			// SingleByte = 0x20, // empty char
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
	bytes,
	charv,
	charnv
)
values (
	:surrogate,
	:name,
	:big_number,
	:small_number,
	:bytes,
	:charv,
	:charnv
)
returning *;")
					.Append ("surrogate", values.Surrogate)
					.Append ("name", values.Name)
					.Append ("big_number", values.BigNumber)
					.Append ("small_number", values.SmallNumber)
					.Append ("bytes", values.Bytes)
					.Append ("charv", values.Charv)
					.Append ("charnv", values.Charnv)
					.Go<TestRecord?> (TestRecord.getTestRecord);
			case DatabaseType.MSSQL:
				return new Connect (MSSQL_CONNECTION_NAME)
					.Query (@"insert into dbo.test (
	surrogate,
	name,
	big_number,
	small_number,
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
	@bytes,
	@charv,
	@charnv
);")
					.Append ("surrogate", values.Surrogate)
					.Append ("name", values.Name)
					.Append ("big_number", values.BigNumber)
					.Append ("small_number", values.SmallNumber)
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
	bytes,
	charv,
	charnv
)
values (
	?surrogate,
	?name,
	?big_number,
	?small_number,
	?bytes,
	?charv,
	?charnv
);")
					.Append ("surrogate", values.Surrogate)
					.Append ("name", values.Name)
					.Append ("big_number", values.BigNumber)
					.Append ("small_number", values.SmallNumber)
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
	public const string POSTGRESQL_CONNECTION_NAME = "TestPostgresql";
	public const string MSSQL_CONNECTION_NAME = "TestMssql";
	public const string MYSQL_CONNECTION_NAME = "TestMariadb";
}

public class Database : IClassFixture<DatabaseFixture>
{

	private readonly DatabaseFixture _fixture;
	private readonly ConnectionManager _cm;

	public Database (
		DatabaseFixture fixture
	) {
		_fixture = fixture;

		_cm = _fixture.ConnectionManager;
	}

	[Fact]
	public void Get_TestRecord_ExpectNone () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME).Query ("select * from public.test where name = 'bar';").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.Null (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME).Query ("select * from dbo.test where name = 'bar';").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.Null (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME).Query ("select * from test where name = 'bar';").Go<TestRecord?> (TestRecord.getTestRecord);

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

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where id = :id;")
					.Append ("id", id)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
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

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where big_number = :big_number;")
					.Append ("big_number", bignum)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
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

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where small_number = :small_number;")
					.Append ("small_number", smallnum)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
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

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where active = :active;")
					.Append ("active", active)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where active = @active;")
					.Append ("active", active)
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Append_PlainString_ExpectSuccess () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where name = :name;")
					.Append ("name", "foo")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where name = @name;")
					.Append ("name", "foo")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select * from test where name = ?name;")
					.Append ("name", "foo")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Append_Varchar_ExpectSuccess () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where charv = @charv;")
					.AppendVarchar ("charv", "varchar")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Append_Nvarchar_ExpectSuccess () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where charnv = @charnv;")
					.AppendNvarchar ("charnv", "nvarchar")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Call_ScalarFunction_ExpectTrue () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
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

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {

				var dt = new DataTable ();
				dt.Columns.Add ("id", typeof (int));
				dt.Rows.Add (1);
				dt.Rows.Add (2);
				dt.Rows.Add (3);

				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
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

	// time to test the new primitives auto-getting
	// this is for things like .Go<int> (); where no func is passed into Go ();
	[Fact]
	public void Funcless_IntSelect_ExpectInt () {

		int id = 1;

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select id from public.test where id = :id;")
					.Append ("id", id)
					.Go<int> ();

				Assert.Equal (record, id);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select * from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<int> ();

				Assert.Equal (record, id);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select * from test where id = ?id;")
					.Append ("id", id)
					.Go<int> ();

				Assert.Equal (record, id);
			}
		}
	}

	// now expect a nullable
	[Fact]
	public void Funcless_IntSelect_ExpectNull () {

		int id = 123;

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select id from public.test where id = :id;")
					.Append ("id", id)
					.Go<int?> ();

				Assert.Null<int> (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select id from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<int?> ();

				Assert.Null<int> (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select id from test where id = ?id;")
					.Append ("id", id)
					.Go<int?> ();

				Assert.Null<int> (record);
			}
		}
	}

	[Fact]
	public void Funcless_BoolSelect_ExpectBool () {

		int id = 1;

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select active from public.test where id = :id;")
					.Append ("id", id)
					.Go<bool> ();

				Assert.True (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select active from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<bool> ();

				Assert.True (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select active from test where id = ?id;")
					.Append ("id", id)
					.Go<bool> ();

				Assert.True (record);
			}
		}
	}

	// now expect a nullable
	[Fact]
	public void Funcless_BoolSelect_ExpectNull () {

		int id = 123;

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select active from public.test where id = :id;")
					.Append ("id", id)
					.Go<bool?> ();

				Assert.Null<bool> (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select active from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<bool?> ();

				Assert.Null<bool> (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select active from test where id = ?id;")
					.Append ("id", id)
					.Go<bool?> ();

				Assert.Null<bool> (record);
			}
		}
	}

	[Fact]
	public void Funcless_StringSelect_ExpectString () {

		int id = 1;
		var result = "foo";

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select name from public.test where id = :id;")
					.Append ("id", id)
					.Go<string> ();

				Assert.Equal (record, result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select name from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<string> ();

				Assert.Equal (record, result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select name from test where id = ?id;")
					.Append ("id", id)
					.Go<string> ();

				Assert.Equal (record, result);
			}
		}
	}

	// now expect a nullable
	[Fact]
	public void Funcless_StringSelect_ExpectNull () {

		int id = 123;

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select name from public.test where id = :id;")
					.Append ("id", id)
					.Go<string?> ();

				Assert.Null (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select name from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<string?> ();

				Assert.Null (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select name from test where id = ?id;")
					.Append ("id", id)
					.Go<string?> ();

				Assert.Null (record);
			}
		}
	}

	[Fact]
	public void Funcless_DateTimeOffsetSelect_ExpectDateTimeOffset () {

		int id = 1;
		var result = new DateTimeOffset (2000, 1, 1, 0, 0, 0, new TimeSpan (-5, 0, 0));

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select created_dt from public.test where id = :id;")
					.Append ("id", id)
					.Go<DateTimeOffset> ();

				Assert.True (record > result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select created_dt from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<DateTimeOffset> ();

				Assert.True (record > result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select created_dt from test where id = ?id;")
					.Append ("id", id)
					.Go<DateTimeOffset> ();

				Assert.True (record > result);
			}
		}
	}

	// now expect a nullable
	[Fact]
	public void Funcless_DateTimeOffsetSelect_ExpectNull () {

		int id = 123;

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select created_dt from public.test where id = :id;")
					.Append ("id", id)
					.Go<DateTimeOffset?> ();

				Assert.Null<DateTimeOffset> (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select created_dt from dbo.test where id = @id;")
					.Append ("id", id)
					.Go<DateTimeOffset?> ();

				Assert.Null<DateTimeOffset> (record);
			}

			if (connection.Value.DatabaseType == DatabaseType.MySql) {
				var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
					.Query ("select created_dt from test where id = ?id;")
					.Append ("id", id)
					.Go<DateTimeOffset?> ();

				Assert.Null<DateTimeOffset> (record);
			}
		}
	}
}
