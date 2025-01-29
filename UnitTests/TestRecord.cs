using System.Data;
using com.janoserdelyi.DataSource;

namespace UnitTests;

public class TestRecord
{
	public int Id { get; set; }
	public required string Surrogate { get; set; }
	public required string Name { get; set; }
	public required long BigNumber { get; set; }
	public required short SmallNumber { get; set; }
	public required byte SingleByte { get; set; }
	public required byte[] Bytes { get; set; }
	public required string Charv { get; set; } // varchar - not testing in postgresql
	public required string Charnv { get; set; } // nvarchar - not testing in postgresql
	public bool Active { get; set; }
	public DateTimeOffset CreatedDt { get; set; }

	private static TestRecord loadObj (
		Command cmd
	) {
		ArgumentNullException.ThrowIfNull (cmd.DRH);

		return new TestRecord () {
			Id = cmd.DRH.GetInt ("id"),
			Surrogate = cmd.DRH.GetString ("surrogate"),
			Name = cmd.DRH.GetString ("name"),
			BigNumber = cmd.DRH.GetLong ("big_number"),
			SmallNumber = cmd.DRH.GetShort ("small_number"),
			SingleByte = cmd.DRH.GetByte ("single_byte"),
			Bytes = cmd.DRH.GetByteArray ("bytes", 1024),
			Charv = cmd.DRH.GetString ("charv"),
			Charnv = cmd.DRH.GetString ("charnv"),
			Active = cmd.DRH.GetBool ("active"),
			CreatedDt = cmd.DRH.GetDateTimeOffset ("created_dt")
		};
	}

	internal static TestRecord? getTestRecord (
			Command cmd
		) {
		using (var dr = cmd.ExecuteReader ()) {
			if (dr.Read ()) {
				return loadObj (cmd);
			}
			return null;
		}
	}

	internal static IList<TestRecord> getTestRecords (
			Command cmd
		) {
		using (var dr = cmd.ExecuteReader ()) {
			var rets = new List<TestRecord> ();
			while (dr.Read ()) {
				rets.Add (loadObj (cmd));
			}
			return rets;
		}
	}
}
