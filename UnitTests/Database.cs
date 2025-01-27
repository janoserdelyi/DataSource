using com.janoserdelyi.DataSource;

namespace UnitTests;

public class Database {

	const string POSTGRESQL_CONNECTION_NAME = "TestPostgresql";
	const string MSSQL_CONNECTION_NAME = "TestMssql";

	static Database () {

	}

	public Database () {
		var cm = ConnectionManager.Instance;

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				// establish a table to test against
				new Connect (POSTGRESQL_CONNECTION_NAME).Query ("drop table if exists public.test;").Go ();

				// create the table with all sorts of data types
				new Connect (POSTGRESQL_CONNECTION_NAME).Query (@"create table public.test (
id int primary key generated always as identity,
surrogate text not null unique,
name text not null,
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
active bit not null default 1,
created_dt datetimeoffset not null default sysdatetimeoffset()
				);").Go ();
			}
		}
	}

	[Fact]
	public void Get_TestRecord_ExpectNone () {
		var cm = ConnectionManager.Instance;

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (POSTGRESQL_CONNECTION_NAME).Query ("select * from public.test;").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.Null (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME).Query ("select * from dbo.test;").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.Null (record);
			}
		}
	}

	[Fact]
	public void Insert_TestRecord_ExpectInsertion () {
		var cm = ConnectionManager.Instance;

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				var record = new Connect (POSTGRESQL_CONNECTION_NAME)
					.Query ("insert into public.test (surrogate, name) values (:surrogate, :name) returning *;")
					.Append ("surrogate", "foo")
					.Append ("name", "foo")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				var record = new Connect (MSSQL_CONNECTION_NAME)
					.Query ("insert into dbo.test (surrogate, name) output inserted.* values (@surrogate, @name);")
					.Append ("surrogate", "foo")
					.Append ("name", "foo")
					.Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}

	[Fact]
	public void Get_TestRecord_ExpectOne () {
		var cm = ConnectionManager.Instance;

		foreach (var connection in cm.Connections) {
			if (connection.Value.DatabaseType == DatabaseType.Postgresql) {
				new Connect (POSTGRESQL_CONNECTION_NAME).Query ("insert into public.test (surrogate, name) values ('foo', 'foo');").Go ();
				var record = new Connect (POSTGRESQL_CONNECTION_NAME).Query ("select * from public.test where name = 'foo';").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
			if (connection.Value.DatabaseType == DatabaseType.MSSQL) {
				new Connect (MSSQL_CONNECTION_NAME).Query ("insert into dbo.test (surrogate, name) values ('foo', 'foo');").Go ();
				var record = new Connect (MSSQL_CONNECTION_NAME).Query ("select * from dbo.test where name = 'foo';").Go<TestRecord?> (TestRecord.getTestRecord);

				Assert.NotNull (record);
			}
		}
	}


}
