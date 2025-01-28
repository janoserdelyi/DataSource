using System.Data.Common;
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
active bit not null default 1,
created_dt datetimeoffset not null default sysdatetimeoffset()
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
		}
	}

	private TestRecord? insertTestRecord (
		DatabaseType dbtype
	) {
		var values = new TestRecord () {
			Surrogate = "foo",
			Name = "foo",
			BigNumber = (long)1,
			SmallNumber = (short)1
		};

		switch (dbtype) {
			case DatabaseType.Postgresql:
				return new Connect (POSTGRESQL_CONNECTION_NAME)
					.Query ("insert into public.test (surrogate, name, big_number, small_number) values (:surrogate, :name, :big_number, :small_number) returning *;")
					.Append ("surrogate", values.Surrogate)
					.Append ("name", values.Name)
					.Append ("big_number", values.BigNumber)
					.Append ("small_number", values.SmallNumber)
					.Go<TestRecord?> (TestRecord.getTestRecord);
			case DatabaseType.MSSQL:
				return new Connect (MSSQL_CONNECTION_NAME)
					.Query ("insert into dbo.test (surrogate, name, big_number, small_number) output inserted.* values (@surrogate, @name, @big_number, @small_number);")
					.Append ("surrogate", values.Surrogate)
					.Append ("name", values.Name)
					.Append ("big_number", values.BigNumber)
					.Append ("small_number", values.SmallNumber)
					.Go<TestRecord?> (TestRecord.getTestRecord);
		}

		return null;
	}

	public ConnectionManager ConnectionManager { get; set; }
	public readonly string POSTGRESQL_CONNECTION_NAME = "TestPostgresql";
	public readonly string MSSQL_CONNECTION_NAME = "TestMssql";
}

public class Database : IClassFixture<DatabaseFixture>
{

	readonly DatabaseFixture fixture;
	readonly ConnectionManager cm;
	readonly string POSTGRESQL_CONNECTION_NAME;
	readonly string MSSQL_CONNECTION_NAME;

	public Database (
		DatabaseFixture fixture
	) {
		this.fixture = fixture;

		cm = fixture.ConnectionManager;
		POSTGRESQL_CONNECTION_NAME = this.fixture.POSTGRESQL_CONNECTION_NAME;
		MSSQL_CONNECTION_NAME = this.fixture.MSSQL_CONNECTION_NAME;
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
		}
	}
}
