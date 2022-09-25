using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.IO.Compression;
using Newtonsoft.Json.Bson;
using System.Threading;
using System.Buffers;
using BC = BCrypt.Net.BCrypt;
using System.Collections.Concurrent;

namespace LesbianDB
{
	public enum PrivilegeLevel : byte{
		Reader = 0, Writer = 1, Admin = 2, Binlog = 3 
	}


	public readonly struct TransactionResult{
		public readonly string[] result;
		public readonly string failure; //null if no failure

		public TransactionResult(string[] result)
		{
			this.result = result;
			failure = null;
		}
		public TransactionResult(string failure)
		{
			result = null;
			this.failure = failure;
		}
	}
	public readonly struct Command{
		public readonly string cmd;
		public readonly ReadOnlyMemory<string> args;

		public Command(string cmd, ReadOnlyMemory<string> args)
		{
			this.cmd = cmd ?? throw new ArgumentNullException(nameof(cmd));
			this.args = args;
		}
	}
	/// <summary>
	/// Represents an async dictionary that supports cache preloading
	/// </summary>
	public interface ICachePreloadingAsyncDictionary : IAsyncDictionary{
		public void Preload(string key);
	}

	public interface IDatabaseConnection : IAsyncDisposable, IDisposable
	{
		
	}

	/// <summary>
	/// The low-level database implementation
	/// </summary>
	public sealed class Database : IAsyncDisposable, IDisposable
	{
		//Privileges
		private readonly struct UserDescriptor{
			public readonly PrivilegeLevel privilegeLevel;
			public readonly string passhash;

			public UserDescriptor(PrivilegeLevel privilegeLevel, string passhash)
			{
				this.privilegeLevel = privilegeLevel;
				this.passhash = passhash;
			}
		}
		private readonly ConcurrentDictionary<string, UserDescriptor> userDescriptors = new ConcurrentDictionary<string, UserDescriptor>();
		public bool CheckUser(string username, string password, out PrivilegeLevel privilegeLevel){
			bool ret = userDescriptors.TryGetValue(username, out UserDescriptor userDescriptor);
			privilegeLevel = userDescriptor.privilegeLevel;
			if(ret){
				ret = BC.EnhancedVerify(password, userDescriptor.passhash, BCrypt.Net.HashType.SHA384);
			}
			return ret;
		}
		/// <summary>
		/// Creates an ephemeral account with administrative privileges that exists until the database is restarted
		/// </summary>
		/// <param name="passhash">The SHA-384 enhanced bcrypt hash of the new account's password</param>
		/// <returns></returns>
		public bool CreateEphemeralAdmin(string username, string passhash){
			return userDescriptors.TryAdd(username, new UserDescriptor(PrivilegeLevel.Admin, passhash ?? throw new ArgumentNullException(nameof(passhash))));
		}
		private readonly struct WriteCheckedCommand
		{
			public readonly string cmd;
			public readonly ReadOnlyMemory<string> args;
			public readonly bool write;

			public WriteCheckedCommand(Command command, bool write)
			{
				cmd = command.cmd;
				args = command.args;
				this.write = write;
			}
		}

		/// <summary>
		/// Preloads a dictionary key into cache
		/// </summary>
		public async void TryPreload(string container, string key){
			await locker.AcquireReaderLock();
			try{
				if(asyncDictionaries.TryGetValue(container, out IAsyncDictionary dict)){
					if(dict is ICachePreloadingAsyncDictionary dict2){
						dict2.Preload(key);
					}
				}
			} finally{
				locker.ReleaseReaderLock();
			}
		}

		//Locking
		private readonly AsyncReaderWriterLock locker = new AsyncReaderWriterLock();

		//Storage engines
		private readonly Func<IAsyncDictionary> dictionaryFactory;
		private readonly Dictionary<string, IAsyncDictionary> asyncDictionaries = new Dictionary<string, IAsyncDictionary>();

		//Binlog
		private readonly Task binlogFlushing;
		private readonly Stream binlog;
		private volatile Task loadBinlog;
		private volatile bool active;
		private readonly int binlogFlushingInterval;

		//Crash handling
		private volatile bool unstable;

		//Whatever
		private static async Task AwaitMultithreadedReads(Queue<object> multithreadedReads, Action<string> action)
		{
			while (multithreadedReads.TryDequeue(out object returned))
			{
				if (returned is null)
				{
					action?.Invoke(null);
				}
				else if (returned is Task<string> tsk)
				{
					action?.Invoke(await tsk);
				}
				else if (returned is string str)
				{
					action?.Invoke(str);
				}
				else
				{
					throw new Exception("Should not reach here");
				}
			}
		}

		private readonly JsonSerializer jsonSerializer;
		private static readonly ArrayPool<byte> arrayPool = ArrayPool<byte>.Create();

		public async Task<TransactionResult> Execute(PrivilegeLevel privilegeLevel, IEnumerable<Command> conditions, IEnumerable<Command> commandsEnumerable)
		{
			if (privilegeLevel < PrivilegeLevel.Binlog)
			{
				Task tsk = loadBinlog;
				if (tsk is { })
				{
					await tsk;
				}
			}

			Queue<string[]> binlogCommands = new Queue<string[]>();
			WriteCheckedCommand[] commands;
			{

				Queue<WriteCheckedCommand> writeCheckedCommands = new Queue<WriteCheckedCommand>();
				bool admin = false;
				foreach (Command command in commandsEnumerable)
				{
					bool dowrite;
					switch (command.cmd)
					{
						case "DSET":
							if (command.args.Length != 3)
							{
								return new TransactionResult("DSET requires 3 arguments");
							}
							dowrite = true;
							break;
						case "DGET":
							if (command.args.Length != 2)
							{
								return new TransactionResult("DGET requires 2 arguments");
							}
							dowrite = false;
							break;
						case "ACREATEUSER":
						case "ACHANGEPASS":
							if (command.args.Length != 2){
								return new TransactionResult(command.cmd + " requires 2 arguments");
							}
							if(command.args.Span[0] is null || command.args.Span[1] is null){
								return new TransactionResult(command.cmd + " doesn't support null arguments");
							}
							admin = true;
							dowrite = true;
							break;
						case "ADELETEUSER":
						case "AGRANTWRITE":
						case "AREVOKEWRITE":
							if (command.args.Length != 1){
								return new TransactionResult(command.cmd + " requires 1 arguments");
							}
							if (command.args.Span[0] is null)
							{
								return new TransactionResult(command.cmd + " doesn't support null arguments");
							}
							admin = true;
							dowrite = true;
							break;
						default:
							return new TransactionResult("Unknown command: " + command.cmd);
					}
					writeCheckedCommands.Enqueue(new WriteCheckedCommand(command, dowrite));
					if(dowrite){
						SerializedCommand serializedCommand = new SerializedCommand();
						serializedCommand.cmd = command.cmd;
						serializedCommand.args = command.args.ToArray();

						binlogCommands.Enqueue(Misc.Command2Array(command));
					}
				}
				if(admin && privilegeLevel < PrivilegeLevel.Admin){
					return new TransactionResult("Administrative privileges required");
				}
				commands = writeCheckedCommands.ToArray();
			}
			commandsEnumerable = null;
			bool write = binlogCommands.Count > 0;
			if (write && privilegeLevel == PrivilegeLevel.Reader)
			{
				return new TransactionResult("Write privileges required");
			}
			byte[] leased = null;
			int binlogExtension = 0;
			Task binwrite = null;
			try{
				if (binlog is { })
				{
					using (Stream memoryStream = new MemoryStream())
					{
						using (Stream deflateStream = new DeflateStream(memoryStream, CompressionLevel.Optimal, true))
						{
#pragma warning disable CS0618 // Type or member is obsolete
							BsonWriter bsonWriter = new BsonWriter(deflateStream);
#pragma warning restore CS0618 // Type or member is obsolete
							jsonSerializer.Serialize(bsonWriter, binlogCommands.ToArray(), typeof(string[][]));
						}
						binlogCommands = null;
						binlogExtension = (int)memoryStream.Position;
						memoryStream.Seek(0, SeekOrigin.Begin);
						leased = arrayPool.Rent(binlogExtension + 4);
						memoryStream.Read(leased, 4, binlogExtension);
					}
					BitConverter.TryWriteBytes(leased.AsSpan(0, 4), (uint)binlogExtension);
				}
			optimistic_relock:
				bool writelocked = false;
				await locker.AcquireReaderLock();
				try
				{
					if (unstable)
					{
						return new TransactionResult("Database restart required");
					}
					{

						//Conditional committing allows us to do optimistic locking
						Queue<Task<bool>> checks = new Queue<Task<bool>>();
						foreach (Command command in conditions)
						{
							switch (command.cmd)
							{
								case "DCHECKEQ":
								case "DCHECKNEQ":
									if (command.args.Length != 3)
									{
										return new TransactionResult(command.cmd + " requires 3 arguments!");
									}
									if (asyncDictionaries.TryGetValue(command.args.Span[0], out IAsyncDictionary tad))
									{
										checks.Enqueue(Misc.CompareAsync(Misc.Reinterpret(tad.TryGetValue(command.args.Span[1])), command.args.Span[2], command.cmd == "DCHECKNEQ"));
									}
									else if (command.args.Span[2] is { })
									{
										//This is a SPECIAL ERROR MESSAGE, DO NOT CHANGE!!!
										return new TransactionResult("REJECTED");
									}
									break;
							}
						}
						conditions = null;
						bool pass = true;
						while (checks.TryDequeue(out Task<bool> tskpass))
						{
							pass &= await tskpass;
						}
						if (!pass)
						{ //This is a SPECIAL ERROR MESSAGE, DO NOT CHANGE!!!
							return new TransactionResult("REJECTED");
						}
					}

					if (write && privilegeLevel < PrivilegeLevel.Binlog)
					{
						writelocked = await locker.TryUpgradeToWriterLock();
						if (!writelocked)
						{
							goto optimistic_relock;
						}
						if (binlog is { })
						{
							binwrite = binlog.WriteAsync(leased, 0, binlogExtension + 4);
							if(binlogFlushingInterval == 0){
								binwrite = Misc.Chain(binlog.FlushAsync, binwrite);
							}
						}
					}

					Queue<string> returnQueue = new Queue<string>();

					Queue<object> multithreadedReads = new Queue<object>();
					bool prevwrite = false;
					foreach (WriteCheckedCommand command in commands)
					{
						if (command.write || prevwrite)
						{
							await AwaitMultithreadedReads(multithreadedReads, returnQueue.Enqueue);
						}
						prevwrite = command.write;
						object returns;
						switch (command.cmd)
						{
							case "DSET":
								{
									string container = command.args.Span[0];
									if (!asyncDictionaries.TryGetValue(container, out IAsyncDictionary val))
									{
										val = dictionaryFactory();
										asyncDictionaries.Add(container, val);
									}
									string set = command.args.Span[2];
									if (set is null)
									{
										returns = Misc.SwitchAsync(val.TryRemove(command.args.Span[1]), "OK", null);
									}
									else
									{
										returns = Misc.ChainValue(val.SetOrAdd(command.args.Span[1], set), "OK");
									}
								}
								break;
							case "DGET":
								{
									if (asyncDictionaries.TryGetValue(command.args.Span[0], out IAsyncDictionary val))
									{
										returns = Misc.Reinterpret(val.TryGetValue(command.args.Span[1]));
									}
									else
									{
										returns = null;
									}
								}
								break;
							case "ACREATEUSER":
								returns = userDescriptors.TryAdd(command.args.Span[0], new UserDescriptor(PrivilegeLevel.Reader, command.args.Span[1])) ? "OK" : null;
								break;
							case "ACHANGEPASS":
								{
									string user = command.args.Span[0];
									if (userDescriptors.TryGetValue(user, out UserDescriptor userDescriptor))
									{
										if (userDescriptor.privilegeLevel < PrivilegeLevel.Admin)
										{
											userDescriptors[user] = new UserDescriptor(userDescriptor.privilegeLevel, command.args.Span[1]);
											returns = "OK";
											break;
										}
									}
								}
								returns = null;
								break;
							case "ADELETEUSER":
								{
									string user = command.args.Span[0];
									if (userDescriptors.TryGetValue(user, out UserDescriptor userDescriptor))
									{
										if (userDescriptor.privilegeLevel < PrivilegeLevel.Admin)
										{
											userDescriptors.TryRemove(user, out _);
											returns = "OK";
											break;
										}
									}
								}
								returns = null;
								break;
							case "AGRANTWRITE":
								{
									string user = command.args.Span[0];
									if (userDescriptors.TryGetValue(user, out UserDescriptor userDescriptor))
									{
										if (userDescriptor.privilegeLevel == PrivilegeLevel.Reader)
										{
											userDescriptors[user] = new UserDescriptor(PrivilegeLevel.Writer, userDescriptor.passhash);
											returns = "OK";
											break;
										}
									}
								}
								returns = null;
								break;
							case "AREVOKEWRITE":
								{
									string user = command.args.Span[0];
									if (userDescriptors.TryGetValue(user, out UserDescriptor userDescriptor))
									{
										if (userDescriptor.privilegeLevel == PrivilegeLevel.Writer)
										{
											userDescriptors[user] = new UserDescriptor(PrivilegeLevel.Reader, userDescriptor.passhash);
											returns = "OK";
											break;
										}
									}
								}
								returns = null;
								break;
							default:
								throw new Exception("Should not reach here");
						}
						if (returns is null || returns is string || returns is Task<string>)
						{
							multithreadedReads.Enqueue(returns);
						}
						else
						{
							throw new Exception("Should not reach here");
						}
					}
					await AwaitMultithreadedReads(multithreadedReads, returnQueue.Enqueue);
					return new TransactionResult(returnQueue.ToArray());
				}
				catch (Exception e)
				{
					if (e is UserError)
					{
						return new TransactionResult(e.Message);
					}
					else
					{

						//An unexpected exception can be an indication that something is seriously wrong
						//For example, and IO exception when accessing swap arenas
						//We mark the database as unstable, and cause all queries to fail until the database is restarted
						unstable = true;
						throw;
					}
				}
				finally
				{
					if (privilegeLevel < PrivilegeLevel.Binlog)
					{
						if (writelocked)
						{
							if(binwrite is { }){
								Task tsk = binwrite;
								binwrite = null;
								try{
									await tsk;
								} catch{
									//Binlog writing failure
									unstable = true;
									throw;
								}
							}
							locker.ReleaseWriterLock();
						}
						else
						{
							locker.ReleaseReaderLock();
						}
					}
				}
			}
			
			finally
			{
				if(write){
					if (binwrite is { })
					{
						try
						{
							await binwrite;
						}
						catch
						{
							//Binlog writing failure
							unstable = true;
							throw;
						}
					}
					if (leased is { })
					{
						arrayPool.Return(leased, false);
					}
				}
			}
		}

		//More binlog methods
		[JsonObject(MemberSerialization.Fields)]
		private sealed class SerializedCommand{
			public string cmd;
			public string[] args;
		}

		public Task WaitForBinlog() => loadBinlog ?? Misc.completed;
		private async Task FlushBinlog()
		{
			CancellationToken cancellationToken = binlogFlushingCancellation.Token;
			{
				Task tsk = loadBinlog;
				if (tsk is { })
				{
					await tsk;
				}
			}
			while (active)
			{
				try
				{
					await Task.Delay(binlogFlushingInterval, cancellationToken);
				}
				catch (TaskCanceledException)
				{
					return;
				}

				await binlog.FlushAsync();
			}
		}
		private async Task LoadBinlog()
		{
			Memory<byte> lengthctr = SmarterMalloc<byte>.Malloc(4);
			JsonSerializer jsonSerializer = new JsonSerializer();
			
			Queue<Command> commands = new Queue<Command>();
			Dictionary<string, string[]> kvp2 = new Dictionary<string, string[]>();
			Stream memorySteam = null;
			byte[] buffer = null;
			try
			{
				int bufsize = 0;
			start:
				long read = await binlog.ReadAsync(lengthctr);
				if (read != 4)
				{
					if (read > 0 && binlog.CanSeek)
					{
						//[SELF HEALING]: Roll back truncated binlog
						binlog.Seek(-read, SeekOrigin.Current);
					}
					goto end;
				}
				long length = BitConverter.ToUInt32(lengthctr.Span);
				if (length > 0)
				{
					read = 0;
					if(buffer is null){
						buffer = arrayPool.Rent(65536);
						bufsize = buffer.Length;
					}
					do
					{
						long toRead = Math.Min(length - read, bufsize);
						long readNow = await binlog.ReadAsync(buffer, 0, (int)toRead);
						if (readNow == 0)
						{
							break; // End of stream
						}
						if(memorySteam is null){
							memorySteam = new MemoryStream();
						}
						memorySteam.Write(buffer, 0, (int)readNow);
						read += readNow;
					} while (read < length);
					if (read != length)
					{
						if(binlog.CanSeek){
							//[SELF HEALING]: Roll back truncated binlog
							binlog.Seek(-4 - read, SeekOrigin.Current);
						}
						goto end;
					}
					//Rewind and truncate memory stream
					memorySteam.Seek(0, SeekOrigin.Begin);
					memorySteam.SetLength(length);
					using (Stream deflate = new DeflateStream(memorySteam, CompressionMode.Decompress, true))
					{
#pragma warning disable CS0618 // Type or member is obsolete
						BsonReader bsonReader = new BsonReader(deflate);
#pragma warning restore CS0618 // Type or member is obsolete
						jsonSerializer.Populate(bsonReader, kvp2);
						int limit = kvp2.Count;
						for(int i = 0; i < limit; ++i){
							string[] arry = kvp2[i.ToString()];
							commands.Enqueue(new Command(arry[0], arry.AsMemory(1)));
						}
						//We use the special binlog privilege level
						//It's like admin with the ability to execute queries while the binlog is still loading
						Task tsk = Execute(PrivilegeLevel.Binlog, EmptyArray<Command>.instance, commands.ToArray());
						kvp2.Clear();
						commands.Clear();
						await tsk;

					}
					memorySteam.Seek(0, SeekOrigin.Begin);
					goto start;
				}
			} finally{
				memorySteam?.Dispose();
				if(buffer is { }){
					arrayPool.Return(buffer, false);
				}
			}
		end:
			loadBinlog = null;
		}

		//Disposal
		private readonly bool leaveOpen;
		private volatile int disposed;
		private readonly CancellationTokenSource binlogFlushingCancellation;
		public async ValueTask DisposeAsync()
		{
			if (Interlocked.Exchange(ref disposed, 1) == 0 && binlog is { })
			{
				GC.SuppressFinalize(this);
				if (binlogFlushing is { })
				{
					binlogFlushingCancellation.Cancel();
					active = false;
					await binlogFlushing;
				}
				if (binlogFlushingInterval > 0)
				{
					await binlog.FlushAsync();
				}
				if (leaveOpen)
				{
					return;
				}
				await binlog.DisposeAsync();
			}
		}
		private void Dispose(bool disposing)
		{
			if (Interlocked.Exchange(ref disposed, 1) == 0 && binlog is { })
			{
				if (disposing)
				{
					GC.SuppressFinalize(this);
				}
				if (binlogFlushing is { })
				{
					binlogFlushingCancellation.Cancel();
					active = false;
					binlogFlushing.Wait();
				}
				if (binlogFlushingInterval > 0)
				{
					binlog.Flush();
				}
				if(leaveOpen){
					return;
				}
				binlog.Dispose();
			}
		}
		~Database()
		{
			Dispose(false);
		}
		public void Dispose()
		{
			Dispose(true);
		}


		//Constructors

		/// <summary>
		/// Creates a persistent database from binlog
		/// </summary>
		/// <param name="binlogFlushingInterval">Interval between binlog flushes in milliseconds. Positive value to enable interval flushing, zero to flush on every transaction, and negative to disable binlog flushing entirely.</param>
		public Database(Func<IAsyncDictionary> dictionaryFactory, Stream binlog, int binlogFlushingInterval, bool leaveOpen)
		{
			if (binlogFlushingInterval < 0 && leaveOpen)
			{
				//[OPTIMIZATION] Don't dispose or finalize if we are not closing or flushing
				GC.SuppressFinalize(this);
				disposed = 1;
			}
			this.dictionaryFactory = dictionaryFactory ?? throw new ArgumentNullException(nameof(dictionaryFactory));
			this.binlog = binlog ?? throw new ArgumentNullException(nameof(binlog));
			this.binlogFlushingInterval = binlogFlushingInterval;
			this.leaveOpen = leaveOpen;

			active = true;
			jsonSerializer = new JsonSerializer();
			Interlocked.MemoryBarrier();
			loadBinlog = LoadBinlog();
			if (binlogFlushingInterval > 0)
			{
				binlogFlushingCancellation = new CancellationTokenSource();
				binlogFlushing = FlushBinlog();
			}
		}

		/// <summary>
		/// Create an ephemeral database that is discarded 
		/// </summary>
		public Database(Func<IAsyncDictionary> dictionaryFactory)
		{
			GC.SuppressFinalize(this);
			this.dictionaryFactory = dictionaryFactory ?? throw new ArgumentNullException(nameof(dictionaryFactory));
		}
	}
}
