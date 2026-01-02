# DataSource

A seriously old (2004'ish?) shim layer for data access that i have been updating and use all the time!

This can be used to connect to Postgresql, MSSQL, and MySQL. There was some stubbing in for SQLite, but it's incomplete.
As of this writing it's really only been used for Postgresql for about 6 years so MSSQL and MySQL are bound to have fallen behind some.

This code can likely use an overhaul, and yes it's ugly. But it does have one thing going for it - it's been used in projects for nearly 20 years in some form or fashion so it has been well-tested!

## Creating Connections

Named connections can be created two ways :
1. XML file
2. Programmatically

### XML file
```xml
<?xml version="1.0" encoding="utf-8" ?>
<configuration>
	<connections>
		
		<connection>
			<name>MyConnection</name>
			<dbtype>postgresql</dbtype>
			<server>some.database.hostname.or.ip.address</server>
			<database>datbase-name-here</database>
			<username>foo</username>
			<password>abc123</password>
			<!-- or<passfile>/path/to/.pgpass</passfile> -->
			<port>5432</port>
		</connection>
		
		<!-- add any number of connections here -->
		
	</connections>
</configuration>
```

### Programmatically
```csharp
ConnectionPropertyBag connectionPropertyBag = new ConnectionPropertyBag () {
	DatabaseType = DatabaseType.Postgresql,
	Name = "MyConnection",
	Server = "some.database.hostname.or.ip.address",
	Database = "datbase-name-here",
	Username = "foo",
	Password = "abc123",
	Port = "5432"
};

ConnectionManager.Instance.AddConnection (connectionPropertyBag);

```

There are more connection options, like timeouts and application name.

Connections are stored in the ConnectionManager Instance singleton keyed by Name


## Usage

### Battle-tested syntax with easy access to everything

inserting a record
```csharp
using (Connection conn = ConnectionManager.Instance.GetConnection ("MyConnection")) {
	using (Command cmd = conn.GetCommand ("insert into foo (some_field) values (:some_field) on conflict (some_field) do nothing;")) {
		cmd.CommandType = CommandType.Text;
		cmd.CH.Append ("some_field", someVariable);
		return cmd.ExecuteScalar ();
	}
}
```

selecting records
```csharp
using (Connection conn = ConnectionManager.Instance.GetConnection ("MyConnection")) {
	using (Command cmd = conn.GetCommand ("select * from foo where bar = :bar;")) {
		cmd.CommandType = CommandType.Text;
		cmd.CH.Append ("bar", barVariable);
		using (IDataReader dr = cmd.ExecuteReader ()) {
			if (dr.Read ()) {
				Console.WriteLine ("id : " + cmd.DRH.GetInt ("id").ToString ());
				Console.WriteLine ("create_dt : " + cmd.DRH.GetDateTime ("create_dt").ToString ());
			}
		}
	}
}
```

The `.Append(paramName, paramValue)` method has many overloads for just about any datatype. There are a few exceptions where types cannot be determined, such as inserting JSON or JSONB since they are strings. For those cases there is `.AppendJson(...)` and `.AppendJsonb(...)`

### New terse syntax

inserting a record
```csharp
new Connect ("MyConnection").Query ("insert into foo (some_field) values (:some_field) on conflict (some_field) do nothing;").Append ("some_field", someVariable).Go();
```

selecting records
to select in a generic fashion, `.Go()` takes a `Func<com.janoserdelyi.DataSource.Command cmd, T>`

```csharp
IList<string> results = new Connect ("MyConnection").Query ("select bar from foo;").Go<IList<string>> (getStrings);

private static IList<string> getStrings (
	com.janoserdelyi.DataSource.Command cmd
) {
	using (IDataReader dr = cmd.ExecuteReader ()) {
		IList<string> rets = new List<string> ();
		while (dr.Read ()) {
			rets.Add (cmd.DRH.GetString ("bar"));
		}
		return rets;
	}
}		
```

If `getStrings` is not something you would use in multiple functions, you can easily inline the Func.

```csharp
IList<string> results = new Connect ("MyConnection").Query ("select bar from foo;").Go<IList<string>> ((cmd) => {
	using (IDataReader dr = cmd.ExecuteReader ()) {
		IList<string> rets = new List<string> ();
		while (dr.Read ()) {
			rets.Add (cmd.DRH.GetString ("bar"));
		}
		return rets;
	}
});		
```


