namespace UnitTests;

public class TestDto
{
	public int Id { get; set; }
	public required string Surrogate { get; set; }
	public required string Name { get; set; }
	public required long BigNumber { get; set; }
	public required short SmallNumber { get; set; }
	public required byte[] Bytes { get; set; }
	public required string Charv { get; set; }
	public required string Charnv { get; set; }
	public required bool Active { get; set; }
	public required DateTimeOffset CreatedDt { get; set; }
}
