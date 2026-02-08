namespace com.janoserdelyi.DataSource;

public class MissingConfigurationException : System.Exception
{
	public MissingConfigurationException () : base () { }
	public MissingConfigurationException (string message) : base (message) {

	}
	public MissingConfigurationException (string message, Exception innerException) : base (message, innerException) {

	}
}
