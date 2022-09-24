using System;
using System.Collections.Generic;
using System.Text;

namespace LesbianDB
{
	/// <summary>
	/// SmarterMalloc: ultimate C# small array allocator
	/// </summary>
	public static class SmarterMalloc<T>
	{
		[ThreadStatic] private static Memory<T> remaining;
		[ThreadStatic] private static bool init;
		public static readonly Memory<T> empty = Memory<T>.Empty;
		public static Memory<T> Malloc(int length)
		{
			if (length == 0)
			{
				return empty;
			}
			else if (length > 256)
			{
				return new T[length];
			}
			else
			{
				if (!init)
				{
					remaining = new T[256];
					init = true;
				}
				int limit = remaining.Length;
				Memory<T> temp = length > limit ? new T[256] : remaining;
				remaining = length == limit ? new T[256] : temp[length..];
				return temp.Slice(0, length);
			}
		}
	}
}
