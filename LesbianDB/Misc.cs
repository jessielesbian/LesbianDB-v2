using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading;
using System.Text;
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography;
using System.IO.MemoryMappedFiles;
using System.Buffers;

namespace LesbianDB
{
	public static class DefaultComparer<T>{
		public static readonly Comparer<T> instance = Comparer<T>.Default;
	}

	public static class EmptyArray<T>{
		public static readonly T[] instance = new T[0];
	}
	public static class Misc
	{
		public static readonly string tempdir = Path.GetTempPath();
		public static string GetRandFileName(){
			Span<byte> bytes = stackalloc byte[32];
			RandomNumberGenerator.Fill(bytes);

			return tempdir + Convert.ToBase64String(bytes).Replace('/', '-');
		}
		public static int Mod(int x, int m)
		{
			x = x % m;
			return x < 0 ? x + m : x;
		}
		public static readonly Task completed = Task.CompletedTask;
		public static Task DoNothing2() => completed;

		public static async Task<T> Reinterpret<T>(Task<ReadResult<T>> tsk){
			ReadResult<T> res = await tsk;
			if(res.exist){
				return res.res;
			} else{
				return default;
			}
		}
		public static async Task<bool> CompareAsync(Task<string> tsk, string val, bool inv)
		{
			return ((await tsk) == val) ^ inv;
		}
		private static readonly ConcurrentBag<byte[]> CopyBuffers = new ConcurrentBag<byte[]>();

		public static T SimpleCreate<T>() where T : new(){
			return new T();
		}

		public static async Task<T> SwitchAsync<T>(Task<bool> tsk, T _true, T _false){
			return (await tsk) ? _true : _false;
		}
		public static async Task<T> ChainValue<T>(Task tsk, T val){
			await tsk;
			return val;
		}

		public static async Task<long> CopyBytes(this Stream inStream, Stream outStream, bool asyncIn, bool asyncOut, long bytesRequired)
		{
			long readSoFar = 0;


			byte[] buffer = null;
			try{
				if (!CopyBuffers.TryTake(out buffer))
				{
					buffer = new byte[65536];
				}
				do
				{
					long toRead = Math.Min(bytesRequired - readSoFar, 65536);
					long readNow = asyncIn ? await inStream.ReadAsync(buffer, 0, (int)toRead) : inStream.Read(buffer, 0, (int) toRead);
					if (readNow == 0){
						break; // End of stream
					} else if(asyncOut){
						await outStream.WriteAsync(buffer, 0, (int)readNow);
					} else{
						outStream.Write(buffer, 0, (int)readNow);
					}
					readSoFar += readNow;
				} while (readSoFar < bytesRequired);
			} finally{
				if(buffer is { }){
					CopyBuffers.Add(buffer);
				}
			}
			return readSoFar;
		}

		public static string[] Command2Array(Command command){
			string[] arr = new string[command.args.Length + 1];
			command.args.CopyTo(arr.AsMemory(1));
			arr[0] = command.cmd;
			return arr;
		}

		public static async Task Chain(Func<Task> func, Task tsk){
			await tsk;
			await func();
		}

		public static readonly Task<bool> completedTrue = Task.FromResult(true);

		public static void DoNothing(){
			
		}

		public static readonly Task<ReadResult<string>> invalidStringRead = Task.FromResult(new ReadResult<string>());

		public static Task PseudoWriteHandler(ReadOnlyMemory<ReadOnlyMemory<string>> readOnlyMemory){
			return completed;
		}
	}
}
