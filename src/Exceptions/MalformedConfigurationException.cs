using System;

namespace com.janoserdelyi.DataSource
{
	public class MalformedConfigurationException : System.Exception
	{
		public MalformedConfigurationException () : base () { }
		public MalformedConfigurationException (string message) : base (message) {

		}
		public MalformedConfigurationException (string message, Exception innerException) : base (message, innerException) {

		}
	}
}