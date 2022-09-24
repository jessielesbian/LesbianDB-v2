using System;
using System.Collections.Generic;
using System.Text;

namespace LesbianDB
{
	public sealed class UserError : Exception
	{
		public UserError()
		{
		}

		public UserError(string message) : base(message)
		{
		}

		public UserError(string message, Exception innerException) : base(message, innerException)
		{
		}
	}

	/// <summary>
	/// This exception is thrown by storage engines to notify the Storage Engine Wrapper that it should return null
	/// </summary>
	public sealed class ReturnNullException : Exception{
		
	}
}
