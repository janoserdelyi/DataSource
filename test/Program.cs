using System;
using System.Collections.Generic;
using System.Data;
using com.janoserdelyi.DataSource;

namespace test
{
	class Program
	{
		static void Main (string[] args) {
			Console.WriteLine ("Hello World!");

			bool newTest = false;

			if (newTest) {

				ConnectionPropertyBag cpb = new ConnectionPropertyBag () {
					DatabaseType = DatabaseType.Postgresql,
					Name = "Test",
					Server = "127.0.0.1",
					Database = "test",
					Username = "janos",
					Password = "foobar",
					Port = "5432"
				};
				ConnectionManager.Instance.AddConnection (cpb);

				new Connect ("Test").Query ("insert into foo (bar) values (:bar);").Append ("bar", "weeeooo").Go ();

				return;
			}


			ConnectionPropertyBag connectionPropertyBag = new ConnectionPropertyBag () {
				DatabaseType = DatabaseType.Postgresql,
				Name = "optional for this",
				Server = "1.2.3.4",
				Database = "SweetDb",
				Username = "foo",
				Password = "bar",
				Port = "5432", // everything is a string with this... (might change that)
				ApplicationName = "Optional foo"
			};

			string bagString = connectionPropertyBag.ToString ();
			string parsedString = ConnectionPropertyBag.Parse (bagString, connectionPropertyBag.DatabaseType).ToString ();

			Console.WriteLine ("connection string built from object compared to parsing that string. they should be the same");
			Console.WriteLine ("from object : " + bagString);
			Console.WriteLine ("from string : " + parsedString);

			// test database started with
			// /usr/pgsql-11/bin/pg_ctl -D /home/janos/pg/11.6/test -l logfile start

			// i really should create a table with fields containing all data types
			// do insertions
			// do selects
			// drop the table

			// let's re-use..
			connectionPropertyBag.Name = "Test";
			connectionPropertyBag.Server = "127.0.0.1";
			connectionPropertyBag.Username = "janos";
			connectionPropertyBag.Database = "test";
			//connectionPropertyBag.Passfile = "/home/janos/.pgpass";
			connectionPropertyBag.Password = "foobar";
			connectionPropertyBag.ApplicationName = "DataSource Test";

			ConnectionManager.Instance.AddConnection (connectionPropertyBag);

			Console.WriteLine ("Connections : ");
			foreach (System.Collections.Generic.KeyValuePair<string, ConnectionPropertyBag> c in ConnectionManager.Instance.Connections) {
				Console.WriteLine ("Name : " + c.Key);
				Console.WriteLine ("Connection string : " + c.Value.ToString ());
				Console.WriteLine ();
			}

			// first, drop the table we're about to create
			try {
				new Connect ("Test").Query ("drop table dts;").Go ();
			} catch (Exception oops) {
				Console.WriteLine (oops.Message);
			}



			string createTableQuery = @"
create table dts (
    id serial primary key,
    create_dt timestamp with time zone not null default(now()),
    a_string text,
    a_nullable_int int,
    an_int int not null default(0),
    a_nullable_jsonb jsonb,
    an_inet inet
);";
			// create table
			new Connect ("Test").Query (createTableQuery).Go ();

			string addRecordQuery = @"
insert into dts (a_string, a_nullable_int, an_int, a_nullable_jsonb, an_inet) 
values (:a_string, :a_nullable_int, :an_int, :a_nullable_jsonb, :an_inet);";

			// add a record
			new Connect ("Test").Query (addRecordQuery)
				.Append ("a_string", "test")
				.Append ("a_nullable_int", (int?)null)
				.Append ("an_int", 1009)
				.AppendJsonb ("a_nullable_jsonb", "{\"foo\":\"bar\"}")
				.Append ("an_inet", System.Net.IPAddress.Parse ("127.0.0.1"))
				.Go ();

			new Connect ("Test").Query (addRecordQuery)
				.Append ("a_string", "test2")
				.Append ("a_nullable_int", 3)
				.Append ("an_int", 1010)
				.AppendJsonb ("a_nullable_jsonb", "{\"foo\":\"bar\"}")
				.Append ("an_inet", System.Net.IPAddress.Parse ("192.168.0.1"))
				.Go ();

			/*
			using (Connection conn = ConnectionManager.Instance.GetConnection ("Test")) {

				using (Command cmd = conn.GetCommand ("select * from dts limit 1;")) {
					cmd.CommandType = CommandType.Text;
					//cmd.CH.Append ("foo", foo);
					using (IDataReader dr = cmd.ExecuteReader ()) {
						if (dr.Read ()) {
							Console.WriteLine ("id : " + cmd.DRH.GetInt ("id").ToString ());
							Console.WriteLine ("create_dt : " + cmd.DRH.GetDateTime ("create_dt").ToString ());
							Console.WriteLine ("a_string : " + cmd.DRH.GetString ("a_string"));
							//Console.WriteLine ("a_nullable_int : " + cmd.DRH.GetIntNullable ("a_nullable_int").Value.ToString ());
							Console.WriteLine ("an_int : " + cmd.DRH.GetInt ("an_int").ToString ());
							Console.WriteLine ("a_nullable_jsonb : " + cmd.DRH.GetJsonb ("a_nullable_jsonb").ToString ());
							Console.WriteLine ("an_inet : " + cmd.DRH.GetIpAddress ("an_inet").ToString ());
						}
					}
				}
			}
			*/
			IList<string> results = new Connect ("Test").Query ("select * from dts;").Go<IList<string>> (getStrings);

			foreach (string result in results) {
				Console.WriteLine (result);
			}

			// =========================================================================================

			// i wonder if i could produce structures to allow for syntax like :
			// insert - 
			// Connect("connection name").Query("insert into...").Append("param1Name", param1Value).Append("param2Name", param2Value);

			// select? not sure how i would want to distingush looping results vs not. ICollection detection?
			// SomeReturnType ret = Connect("connection name").Query<return type>("insert into...").Append("param1Name", param1Value).Append("param2Name", param2Value);

		}

		private static System.Collections.Generic.IList<string> getStrings (
			com.janoserdelyi.DataSource.Command cmd
		) {
			using (IDataReader dr = cmd.ExecuteReader ()) {
				System.Collections.Generic.IList<string> rets = new System.Collections.Generic.List<string> ();
				while (dr.Read ()) {
					rets.Add (cmd.DRH.GetString ("a_string"));
				}
				return rets;
			}
		}
	}
}
