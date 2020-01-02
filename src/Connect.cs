using System;

namespace com.janoserdelyi.DataSource
{
	public class Connect
	{
		// i wonder if i could produce structures to allow for syntax like :
		// insert - 
		// new Connect("connection name").Query("insert into...").Append("param1Name", param1Value).Append("param2Name", param2Value).Go();

		// select? not sure how i would want to distingush looping results vs not. ICollection detection?
		// SomeReturnType ret = new Connect("connection name").Query("insert into...").Append("param1Name", param1Value).Append("param2Name", param2Value).Go<return type>();

		/*

		*/
		private string connectionName { get; set; }
		private string query { get; set; }
		private Connection connection { get; set; }
		private Command command { get; set; }

		public Connect (
			string connectionName
		) {
			this.connectionName = connectionName;
			this.connection = ConnectionManager.Instance.GetConnection (connectionName, false);
		}

		public Connect Query (
			string query
		) {
			this.query = query;
			this.command = this.connection.GetCommand (query);
			this.command.CommandType = System.Data.CommandType.Text;
			return this;
		}

		public void Go () {
			this.connection.OpenAsync ();
			using (this.connection) {
				using (this.command) {
					this.command.ExecuteNonQueryAsync ();
				}
			}
		}

		// i'm considering requiring an interface on this 
		// and likely take a func<> for loading the object
		public T Go<T> (Func<com.janoserdelyi.DataSource.Command, T> loadObj) { //where T : new() {

			this.connection.OpenAsync ();
			using (this.connection) {
				using (this.command) {
					//using (System.Data.IDataReader dr = this.command.ExecuteReader ()) {
					//T rets = new T ();
					//T rets = (T)Activator.CreateInstance<T> ();
					//while (dr.Read ()) {
					//	rets.Add ("foo");
					//}
					return loadObj (this.command);
					//}
				}
			}

			//object foo = "weee";
			//return (T)Convert.ChangeType (foo, typeof (T));
		}

		public Connect Append (
			string name,
			string value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			int value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			long value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			DateTime value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			short value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			bool value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			byte value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			TimeSpan value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			Guid value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			string[] value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			int[] value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			int[,] value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			long[] value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			double[] value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			DateTime[] value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			System.Net.IPAddress value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect AppendJson (
			string name,
			string value
		) {
			this.command.CH.AppendJson (name, value);
			return this;
		}

		public Connect AppendJsonb (
			string name,
			string value
		) {
			this.command.CH.AppendJsonb (name, value);
			return this;
		}

		public Connect Append (
			string name,
			int? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			long? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			DateTime? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			Guid? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			decimal? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			short? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			bool? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			byte? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			char? value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			System.Collections.BitArray value
		) {
			this.command.CH.Append (name, value);
			return this;
		}

		public Connect Append (
			string name,
			System.Collections.BitArray value,
			int size
		) {
			this.command.CH.Append (name, value, size);
			return this;
		}


	}
}