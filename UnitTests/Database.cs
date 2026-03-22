using System.Data;
// using System.Data.Common;
using System.Reflection;
using System.Text;
using com.janoserdelyi.DataSource;

namespace UnitTests;

// https://xunit.net/docs/shared-context
public class DatabaseFixture : IDisposable
{
	public DatabaseFixture () {
		ConnectionManager = ConnectionManager.Instance;

		// enable snake_case -> PascalCase mapping for Dapper DTO tests
		Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;

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

		// create a simple scalar function for testing
		_ = new Connect (POSTGRESQL_CONNECTION_NAME).Query ("create or replace function public.is_positive(n int) returns boolean language sql as $$ select n > 0 $$;").Go ();

		// MSSQL
		// establish a table to test against
		_ = new Connect (MSSQL_CONNECTION_NAME).Query ("drop table if exists dbo.test;").Go ();

		// create the table with all sorts of data types
		_ = new Connect (MSSQL_CONNECTION_NAME).Query (@"create table dbo.test (
	id int primary key identity,
	surrogate varchar(20) not null unique,
	name nvarchar(100) not null,
	big_number bigint not null,
	small_number smallint not null,
	bytes varbinary(1024) not null,
	charv varchar(25) not null,
	charnv nvarchar(25) not null,
	active bit not null default 1,
	created_dt datetimeoffset not null default sysdatetimeoffset()
);").Go ();

		_ = insertTestRecord (DatabaseType.MSSQL);

		// create a simple scalar function for testing
		_ = new Connect (MSSQL_CONNECTION_NAME).Query ("drop function if exists dbo.IsPositive;").Go ();
		_ = new Connect (MSSQL_CONNECTION_NAME).Query ("create function dbo.IsPositive(@n int) returns bit as begin return case when @n > 0 then 1 else 0 end end;").Go ();
		// create a user defined type for testing
		_ = new Connect (MSSQL_CONNECTION_NAME).Query ("drop type dbo.IdArrayType;").Go ();
		_ = new Connect (MSSQL_CONNECTION_NAME).Query ("create type dbo.IdArrayType as table (id int);").Go ();

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
				_ = new Connect (POSTGRESQL_CONNECTION_NAME).Query ("drop function if exists public.is_positive(int);").Go ();
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				// establish a table to test against
				_ = new Connect (MSSQL_CONNECTION_NAME).Query ("drop table if exists dbo.test;").Go ();
				_ = new Connect (MSSQL_CONNECTION_NAME).Query ("drop function if exists dbo.IsPositive;").Go ();
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

			string? query = null;
			string? schemafunc = null;
			string? conname = null;
			switch (connection.Value.DatabaseType) {
				case DatabaseType.Postgresql:
					conname = DatabaseFixture.POSTGRESQL_CONNECTION_NAME;
					schemafunc = "public.is_positive";
					query = $"select {schemafunc}(:n) as result;";
					break;
				case DatabaseType.MSSQL:
					conname = DatabaseFixture.MSSQL_CONNECTION_NAME;
					schemafunc = "dbo.IsPositive";
					query = $"select {schemafunc}(@n) as result;";
					break;
				case DatabaseType.MySql:
				case DatabaseType.Sqlite:
				default:
					continue;
			}

			var result = new Connect (conname)
				.Query (query)
				.Append ("n", 1)
				.Go<bool> ((cmd) => {
					using (var dr = cmd.ExecuteReader ()) {
						ArgumentNullException.ThrowIfNull (cmd.DRH);

						if (dr.Read ()) {
							return cmd.DRH.GetBool ("result");
						}

						throw new NullReferenceException ($"error calling {schemafunc}");
					}
				});

			Assert.True (result);
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

				// var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
				// 	.Query ("select id from @ids;")
				// 	.Append ("ids", dt, "dbo.IdArrayType")
				// 	.Go<List<int>> ((cmd) => {
				// 		using (var dr = cmd.ExecuteReader ()) {
				// 			ArgumentNullException.ThrowIfNull (cmd.DRH);

				// 			var rets = new List<int> ();
				// 			while (dr.Read ()) {
				// 				rets.Add (cmd.DRH.GetInt ("id"));
				// 			}

				// 			return rets;
				// 		}
				// 	});

				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select id from @ids;")
					.Append ("ids", dt, "dbo.IdArrayType")
					.Go<List<int>> ();

				Assert.NotNull (result);
				Assert.NotEmpty (result);
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

		var result = new DateTimeOffset (2000, 1, 1, 0, 0, 0, new TimeSpan (-5, 0, 0));

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select now() as now;")
					.Go<DateTimeOffset> ();

				Assert.True (record > result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select sysdatetimeoffset() as now;")
					.Go<DateTimeOffset> ();

				Assert.True (record > result);
			}

			// 2026-03-20 mariadb is still trash. from what i can tell there is no date time with timezone
			// if (connection.Value.DatabaseType == DatabaseType.MySql) {
			// 	var record = new Connect (DatabaseFixture.MYSQL_CONNECTION_NAME)
			// 		.Query ("select created_dt from test where id = ?id;")
			// 		.Go<DateTimeOffset> ();

			// 	Assert.True (record > result);
			// }
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

	[Fact]
	public void Funcless_GoListOfInt_WithData_ExpectList () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from (values (1::int), (2::int), (3::int)) as t(n);")
					.Go<List<int>> ();

				Assert.NotNull (result);
				Assert.Equal (3, result.Count);
				Assert.Equal (new List<int> { 1, 2, 3 }, result);
			}
		}
	}

	[Fact]
	public void Funcless_GoIntArray_WithData_ExpectArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from (values (1::int), (2::int), (3::int)) as t(n);")
					.Go<int[]> ();

				Assert.NotNull (result);
				Assert.Equal (3, result.Length);
				Assert.Equal (new int[] { 1, 2, 3 }, result);
			}
		}
	}

	[Fact]
	public void Funcless_GoIEnumerableOfInt_WithData_ExpectResults () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from (values (1::int), (2::int), (3::int)) as t(n);")
					.Go<IEnumerable<int>> ();

				Assert.NotNull (result);
				Assert.Equal (3, result.Count ());
			}
		}
	}

	[Fact]
	public void Funcless_GoListOfString_WithData_ExpectList () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select name from public.test;")
					.Go<List<string>> ();

				Assert.NotNull (result);
				Assert.NotEmpty (result);
				Assert.Contains ("foo", result);
			}
		}
	}

	[Fact]
	public void Funcless_GoStringArray_WithData_ExpectArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select name from public.test;")
					.Go<string[]> ();

				Assert.NotNull (result);
				Assert.NotEmpty (result);
				Assert.Contains ("foo", result);
			}
		}
	}

	[Fact]
	public void Funcless_GoListOfInt_EmptyResult_ExpectEmptyList () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select id from public.test where id = :id;")
					.Append ("id", 99999)
					.Go<List<int>> ();

				Assert.NotNull (result);
				Assert.Empty (result);
			}
		}
	}

	[Fact]
	public void Funcless_GoIntArray_EmptyResult_ExpectEmptyArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select id from public.test where id = :id;")
					.Append ("id", 99999)
					.Go<int[]> ();

				Assert.NotNull (result);
				Assert.Empty (result);
			}
		}
	}

	[Fact]
	public void Funcless_GoTestDto_ExpectDto () {
		int id = 1;

		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where id = :id;")
					.Append ("id", id)
					.Go<TestDto> ();

				Assert.NotNull (result);
				Assert.Equal (id, result.Id);
				Assert.Equal ("foo", result.Name);
				Assert.Equal ("foo", result.Surrogate);
				Assert.Equal (1L, result.BigNumber);
				Assert.Equal ((short)1, result.SmallNumber);
				Assert.True (result.Active);
				Assert.True (result.CreatedDt > DateTimeOffset.UnixEpoch);
			}
		}
	}

	[Fact]
	public void Funcless_GoListOfTestDto_ExpectList () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test;")
					.Go<List<TestDto>> ();

				Assert.NotNull (result);
				Assert.NotEmpty (result);
				Assert.All (result, dto => Assert.NotNull (dto.Name));
			}
		}
	}

	[Fact]
	public void DataReaderHelperPostgresql_OrdinalCaching_CachesOnFirstAccess () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where id = :id;")
					.Append ("id", 1)
					.Go<bool> ((cmd) => {
						using var dr = cmd.ExecuteReader ();

						Assert.True (dr.Read (), "Expected at least one row in public.test");

						var drh = (DataReaderHelperPostgresql)cmd.DRH!;

						var cacheField = typeof (DataReaderHelperBase).GetField (
							"_ordinalCache",
							BindingFlags.NonPublic | BindingFlags.Instance
						);
						var cache = (Dictionary<string, int>)cacheField!.GetValue (drh)!;

						// cache is empty before any column access
						Assert.Empty (cache);

						// first access populates the cache
						_ = drh.GetInt ("id");
						Assert.Single (cache);
						Assert.True (cache.ContainsKey ("id"));
						int cachedOrdinal = cache["id"];

						// repeated access to the same column does not grow the cache
						_ = drh.GetInt ("id");
						Assert.Single (cache);
						Assert.Equal (cachedOrdinal, cache["id"]);

						// accessing a new column grows the cache by exactly one
						_ = drh.GetString ("name");
						Assert.Equal (2, cache.Count);
						Assert.True (cache.ContainsKey ("name"));

						// repeated access to the second column does not grow the cache further
						_ = drh.GetString ("name");
						Assert.Equal (2, cache.Count);

						return true;
					});
			}
		}
	}

	[Fact]
	public void GetLong_ExpectLong () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select big_number from public.test where id = :id;")
					.Append ("id", 1)
					.Go<long> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetLong ("big_number");
					});
				Assert.Equal (1L, result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select big_number from dbo.test where id = @id;")
					.Append ("id", 1)
					.Go<long> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetLong ("big_number");
					});
				Assert.Equal (1L, result);
			}
		}
	}

	[Fact]
	public void GetLong_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::bigint as big_number;")
					.Go<long?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetLong ("big_number", true);
					});
				Assert.Null (result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select CAST(NULL AS bigint) as big_number;")
					.Go<long?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetLong ("big_number", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetShort_ExpectShort () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select small_number from public.test where id = :id;")
					.Append ("id", 1)
					.Go<short> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetShort ("small_number");
					});
				Assert.Equal ((short)1, result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select small_number from dbo.test where id = @id;")
					.Append ("id", 1)
					.Go<short> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetShort ("small_number");
					});
				Assert.Equal ((short)1, result);
			}
		}
	}

	[Fact]
	public void GetShort_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::smallint as small_number;")
					.Go<short?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetShort ("small_number", true);
					});
				Assert.Null (result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select CAST(NULL AS smallint) as small_number;")
					.Go<short?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetShort ("small_number", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetByteArray_ExpectByteArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select bytes from public.test where id = :id;")
					.Append ("id", 1)
					.Go<byte[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetByteArray ("bytes", 1024);
					});
				Assert.NotNull (result);
				Assert.Equal (1024, result.Length);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select bytes from dbo.test where id = @id;")
					.Append ("id", 1)
					.Go<byte[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetByteArray ("bytes", 1024);
					});
				Assert.NotNull (result);
				Assert.Equal (1024, result.Length);
			}
		}
	}

	[Fact]
	public void GetByteArray_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::bytea as bytes;")
					.Go<byte[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetByteArray ("bytes", 0, true);
					});
				Assert.Null (result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select CAST(NULL AS varbinary(1)) as bytes;")
					.Go<byte[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetByteArray ("bytes", 0, true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetDateTime_ExpectDateTime () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select now()::timestamp as dt;")
					.Go<DateTime> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDateTime ("dt");
					});
				Assert.True (result > new DateTime (2000, 1, 1));
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select GETDATE() as dt;")
					.Go<DateTime> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDateTime ("dt");
					});
				Assert.True (result > new DateTime (2000, 1, 1));
			}
		}
	}

	[Fact]
	public void GetDateTime_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::timestamp as dt;")
					.Go<DateTime?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDateTime ("dt", true);
					});
				Assert.Null (result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select CAST(NULL AS datetime) as dt;")
					.Go<DateTime?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDateTime ("dt", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void HasField_ExistingColumn_ExpectTrue () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where id = :id;")
					.Append ("id", 1)
					.Go<bool> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.HasField ("id");
					});
				Assert.True (result);
			}
		}
	}

	[Fact]
	public void HasField_MissingColumn_ExpectFalse () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select * from public.test where id = :id;")
					.Append ("id", 1)
					.Go<bool> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.HasField ("nonexistent_column");
					});
				Assert.False (result);
			}
		}
	}

	[Fact]
	public void GetDecimal_ExpectDecimal () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select 1.5::numeric as n;")
					.Go<decimal> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDecimal ("n");
					});
				Assert.Equal (1.5m, result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select CAST(1.5 AS DECIMAL(10,2)) AS n;")
					.Go<decimal> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDecimal ("n");
					});
				Assert.Equal (1.5m, result);
			}
		}
	}

	[Fact]
	public void GetDecimal_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::numeric as n;")
					.Go<decimal?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDecimal ("n", true);
					});
				Assert.Null (result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select CAST(NULL AS decimal(10,2)) as n;")
					.Go<decimal?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDecimal ("n", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetDouble_ExpectDouble () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select 1.5::float8 as n;")
					.Go<double> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDouble ("n");
					});
				Assert.Equal (1.5, result);
			}
		}
	}

	[Fact]
	public void GetDouble_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::float8 as n;")
					.Go<double?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDouble ("n", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetGuid_ExpectGuid () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select gen_random_uuid() as id;")
					.Go<Guid> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetGuid ("id");
					});
				Assert.NotEqual (Guid.Empty, result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select NEWID() as id;")
					.Go<Guid> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetGuid ("id");
					});
				Assert.NotEqual (Guid.Empty, result);
			}
		}
	}

	[Fact]
	public void GetGuid_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::uuid as id;")
					.Go<Guid?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetGuid ("id", true);
					});
				Assert.Null (result);
			}

			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var result = new Connect (DatabaseFixture.MSSQL_CONNECTION_NAME)
					.Query ("select CAST(NULL AS uniqueidentifier) as id;")
					.Go<Guid?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetGuid ("id", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetTimeSpan_ExpectTimeSpan () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select '09:00:00'::time as t;")
					.Go<TimeSpan> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetTimeSpan ("t");
					});
				Assert.Equal (new TimeSpan (9, 0, 0), result);
			}
		}
	}

	[Fact]
	public void GetTimeSpan_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::time as t;")
					.Go<TimeSpan?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetTimeSpan ("t", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetIntArray_ExpectIntArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select ARRAY[1,2,3]::int[] as arr;")
					.Go<int[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetIntArray ("arr");
					});
				Assert.NotNull (result);
				Assert.Equal (new int[] { 1, 2, 3 }, result);
			}
		}
	}

	[Fact]
	public void GetIntArray2D_ExpectIntArray2D () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select ARRAY[[1,2],[3,4]] as arr;")
					.Go<int[,]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetIntArray2D ("arr");
					});
				Assert.NotNull (result);
				Assert.Equal (2, result.GetLength (0));
				Assert.Equal (2, result.GetLength (1));
				Assert.Equal (1, result[0, 0]);
				Assert.Equal (4, result[1, 1]);
			}
		}
	}

	[Fact]
	public void GetLongArray_ExpectLongArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select ARRAY[1,2,3]::bigint[] as arr;")
					.Go<long[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetLongArray ("arr");
					});
				Assert.NotNull (result);
				Assert.Equal (new long[] { 1L, 2L, 3L }, result);
			}
		}
	}

	[Fact]
	public void GetDoubleArray_ExpectDoubleArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select ARRAY[1.5,2.5]::float8[] as arr;")
					.Go<double[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetDoubleArray ("arr");
					});
				Assert.NotNull (result);
				Assert.Equal (new double[] { 1.5, 2.5 }, result);
			}
		}
	}

	[Fact]
	public void GetStringArray_ExpectStringArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select ARRAY['a','b','c']::text[] as arr;")
					.Go<string[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetStringArray ("arr");
					});
				Assert.NotNull (result);
				Assert.Equal (new string[] { "a", "b", "c" }, result);
			}
		}
	}

	[Fact]
	public void GetStringArray_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::text[] as arr;")
					.Go<string[]?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetStringArray ("arr");
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetJson_ExpectJson () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select '{\"key\":\"value\"}'::json as j;")
					.Go<string?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetJson ("j");
					});
				Assert.NotNull (result);
				Assert.Contains ("key", result);
			}
		}
	}

	[Fact]
	public void GetJson_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::json as j;")
					.Go<string?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetJson ("j", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetJsonb_ExpectJsonb () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select '{\"key\":\"value\"}'::jsonb as j;")
					.Go<string?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetJsonb ("j");
					});
				Assert.NotNull (result);
				Assert.Contains ("key", result);
			}
		}
	}

	[Fact]
	public void GetJsonb_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::jsonb as j;")
					.Go<string?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetJsonb ("j", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetInet_ExpectString () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select '192.168.1.1'::inet as addr;")
					.Go<string?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetInet ("addr");
					});
				Assert.NotNull (result);
				Assert.Contains ("192.168.1.1", result);
			}
		}
	}

	[Fact]
	public void GetInet_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::inet as addr;")
					.Go<string?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetInet ("addr", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetIpAddress_ExpectIpAddress () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select '10.0.0.1'::inet as addr;")
					.Go<System.Net.IPAddress?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetIpAddress ("addr");
					});
				Assert.NotNull (result);
				Assert.Equal (System.Net.IPAddress.Parse ("10.0.0.1"), result);
			}
		}
	}

	[Fact]
	public void GetIpAddress_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::inet as addr;")
					.Go<System.Net.IPAddress?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetIpAddress ("addr", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetBit_ExpectBitArray () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select B'101'::varbit as b;")
					.Go<System.Collections.BitArray?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetBit ("b");
					});
				Assert.NotNull (result);
				Assert.Equal (3, result.Length);
			}
		}
	}

	[Fact]
	public void GetBit_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::varbit as b;")
					.Go<System.Collections.BitArray?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetBit ("b", true);
					});
				Assert.Null (result);
			}
		}
	}

	[Fact]
	public void GetRegconfig_ExpectString () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select 'english'::regconfig as cfg;")
					.Go<string?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetRegconfig ("cfg");
					});
				Assert.NotNull (result);
				Assert.NotEmpty (result);
			}
		}
	}

	[Fact]
	public void GetRegconfig_Nullable_ExpectNull () {
		foreach (var connection in _cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var result = new Connect (DatabaseFixture.POSTGRESQL_CONNECTION_NAME)
					.Query ("select NULL::regconfig as cfg;")
					.Go<string?> ((cmd) => {
						using var dr = cmd.ExecuteReader ();
						ArgumentNullException.ThrowIfNull (cmd.DRH);
						dr.Read ();
						return cmd.DRH.GetRegconfig ("cfg");
					});
				Assert.Null (result);
			}
		}
	}
}
