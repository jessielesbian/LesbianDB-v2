using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using System.IO.Compression;
using System.Buffers;
using System.Security.Cryptography;
using System.Collections.Concurrent;
using System.Linq;

namespace LesbianDB
{
	public sealed class JsonArrayPool<T> : IArrayPool<T>
	{
		private readonly ArrayPool<T> arrayPool = ArrayPool<T>.Create();
		public T[] Rent(int minimumLength)
		{
			return arrayPool.Rent(minimumLength);
		}

		public void Return(T[] array)
		{
			arrayPool.Return(array, false);
		}
	}
	public readonly struct ReadResult<T>{
		public readonly T res;
		public readonly bool exist;

		public ReadResult(T res, bool exist)
		{
			this.res = res;
			this.exist = exist;
		}
	}

	public interface IObjectPagingEngine{
		public Task<Func<Task<T>>> CreateHandle<T>(T obj, Func<T> creator);
	}

	public sealed class AsynchronousBinaryPagingEngine : IObjectPagingEngine
	{
		private readonly Func<Stream, Task<Func<Stream, Task>>> func;

		public AsynchronousBinaryPagingEngine(Func<Stream, Task<Func<Stream, Task>>> func)
		{
			this.func = func ?? throw new ArgumentNullException(nameof(func));
		}

		public async Task<Func<Task<T>>> CreateHandle<T>(T obj, Func<T> creator)
		{
			JsonSerializer jsonSerializer = new JsonSerializer();
			int sizehint;
			Func<Stream, Task> read;
			using (MemoryStream memoryStream = new MemoryStream())
			{
				using (Stream deflate = new DeflateStream(memoryStream, CompressionLevel.Optimal, true))
				{
#pragma warning disable CS0618 // Type or member is obsolete
					BsonWriter bsonWriter = new BsonWriter(deflate);
#pragma warning restore CS0618 // Type or member is obsolete
					jsonSerializer.Serialize(bsonWriter, obj);
				}
				sizehint = (int)memoryStream.Position;
				memoryStream.Seek(0, SeekOrigin.Begin);
				read = await func(memoryStream);
			}

			return async () => {
				using MemoryStream memoryStream = new MemoryStream(sizehint);
				await read(memoryStream);
				memoryStream.Seek(0, SeekOrigin.Begin);
				using Stream deflate = new DeflateStream(memoryStream, CompressionMode.Decompress, true);
#pragma warning disable CS0618 // Type or member is obsolete
				BsonReader bsonReader = new BsonReader(deflate);
				if (creator is null)
				{
					return jsonSerializer.Deserialize<T>(bsonReader);
				}
				else
				{
					T n = creator();
					jsonSerializer.Populate(bsonReader, n);
					return n;
				}


#pragma warning restore CS0618 // Type or member is obsolete
			};
		}
	}

	public sealed class SynchronousBinaryPagingEngine : IObjectPagingEngine{
		private readonly Func<ReadOnlyMemory<byte>, Action<Stream>> func;

		public SynchronousBinaryPagingEngine(Func<ReadOnlyMemory<byte>, Action<Stream>> func)
		{
			this.func = func ?? throw new ArgumentNullException(nameof(func));
		}

		private sealed class UpgradingBinaryHandle{
			public volatile Action<Stream> action;
			private volatile Func<ReadOnlyMemory<byte>, Action<Stream>> func;
			public UpgradingBinaryHandle(byte[] bytes, Func<ReadOnlyMemory<byte>, Action<Stream>> func)
			{
				action = (Stream str) => str.Write(bytes, 0, bytes.Length);
				this.func = func;
				ThreadPool.QueueUserWorkItem(Upgrade, bytes, true);
			}
			private void Upgrade(byte[] bytes)
			{
				int ulen = bytes.Length;
				int len;
				WeakReference<byte[]> byteswr = new WeakReference<byte[]>(bytes, false);
				using (MemoryStream dest = new MemoryStream()){
					using (Stream deflateStream = new DeflateStream(dest, CompressionLevel.Optimal, true)){
						deflateStream.Write(bytes, 0, bytes.Length);
						deflateStream.Flush();
					}
					int cs = (int)dest.Position;
					if (cs < ulen){
						bytes = dest.ToArray();
						len = cs;
					} else{
						len = ulen;
					}
				}

				Action<Stream> next = func(bytes);
				func = null;
				bytes = null;
				

				if (len < ulen){
					Action<Stream> next2 = next;
					next = (Stream str) =>
					{
						using Stream to = new MemoryStream(len);
						next2(to);
						to.Seek(0, SeekOrigin.Begin);
						using Stream deflateStream = new DeflateStream(to, CompressionMode.Decompress, true);
						deflateStream.CopyTo(str);
					};
				}

				action = (Stream str) => {
					if (byteswr.TryGetTarget(out byte[] bytes))
					{
						str.Write(bytes, 0, ulen);
					}
					else
					{
						action = next;
						next(str);
					}
				};
			}
		}

		public Task<Func<Task<T>>> CreateHandle<T>(T obj, Func<T> creator)
		{
			
			UpgradingBinaryHandle upgradingBinaryHandle;
			int sizehint;
			{
				byte[] arr;
				using (MemoryStream memoryStream = new MemoryStream())
				{
					JsonSerializer jsonSerializer = new JsonSerializer();
#pragma warning disable CS0618 // Type or member is obsolete
					BsonWriter bsonWriter = new BsonWriter(memoryStream);
#pragma warning restore CS0618 // Type or member is obsolete
					jsonSerializer.Serialize(bsonWriter, obj);
					arr = memoryStream.ToArray();
				}
				sizehint = arr.Length;
				upgradingBinaryHandle = new UpgradingBinaryHandle(arr, func);
			}
			
			return Task.FromResult<Func<Task<T>>>(() => {
				JsonSerializer jsonSerializer = new JsonSerializer();
				using Stream memoryStream = new MemoryStream(sizehint);
				upgradingBinaryHandle.action(memoryStream);
				memoryStream.Seek(0, SeekOrigin.Begin);
#pragma warning disable CS0618 // Type or member is obsolete
				BsonReader bsonReader = new BsonReader(memoryStream);
#pragma warning restore CS0618 // Type or member is obsolete
				if(creator is null){
					return Task.FromResult(jsonSerializer.Deserialize<T>(bsonReader));
				} else{
					T temp = creator();
					jsonSerializer.Populate(bsonReader, temp);
					return Task.FromResult(temp);
				}
			});
			
		}
	}

	public interface IAsyncDictionary{
		public Task<ReadResult<string>> TryGetValue(string key);
		public Task<bool> TryAdd(string key, string value);
		public Task<bool> TryRemove(string key);
		public Task SetOrAdd(string key, string value);

		public Task<bool> ForEach(Func<string, string, Task<bool>> func, bool multithreaded);
	}

	public readonly struct GetRandPairRes<K, V>{
		public readonly K key;
		public readonly V value;
		public readonly bool success;

		public GetRandPairRes(K key, V value, bool success)
		{
			this.key = key;
			this.value = value;
			this.success = success;
		}
	}

	/// <summary>
	/// A shadow dictionary 
	/// </summary>
	public sealed class ShadowDictionary : IAsyncDictionary{
		private sealed class WriteBackDictionary : Dictionary<string, string>{
			private readonly ShadowDictionary shadowDictionary;

			public WriteBackDictionary(ShadowDictionary shadowDictionary)
			{
				this.shadowDictionary = shadowDictionary;
			}

			~WriteBackDictionary(){
				Free2();
			}
			private async void Free2()
			{
				AsyncMutex asyncMutex = shadowDictionary.asyncMutex;
				await asyncMutex.Enter();
				try{
					shadowDictionary.pagedvalue = await shadowDictionary.objectPagingEngine(this);
				} finally{
					asyncMutex.Exit();
				}
				GC.KeepAlive(this);
			}
		}
		
		private readonly Func<IDictionary<string, string>, Task<Func<Task<IDictionary<string, string>>>>> objectPagingEngine;
		private static readonly DictionaryPool<string, string> dictionaryPool = new DictionaryPool<string, string>();
		private readonly WeakReference<IDictionary<string, string>> dict;
		private readonly AsyncMutex asyncMutex = new AsyncMutex();
		private volatile Func<Task<IDictionary<string, string>>> pagedvalue;

		private IDictionary<string, string> Create() => new WriteBackDictionary(this);

		public ShadowDictionary(IObjectPagingEngine pagingEngine){
			objectPagingEngine = (IDictionary<string, string> d) => pagingEngine.CreateHandle(d, Create);
			dict = new WeakReference<IDictionary<string, string>>(new WriteBackDictionary(this));
		}

		private async Task<T> Borrow<T>(Func<IDictionary<string, string>, Task<T>> callback){
		start:
			await asyncMutex.Enter();
			IDictionary<string, string> res;
			try{
				if(dict.TryGetTarget(out res)){
					return await callback(res);
				} else{
					if (pagedvalue is { })
					{
						res = await pagedvalue();
						pagedvalue = null;
						dict.SetTarget(res);
						return await callback(res);
					}
				}
			} finally{
				asyncMutex.Exit();
			}
			goto start;

		}

		public Task<ReadResult<string>> TryGetValue(string key){
			return Borrow((IDictionary<string, string> r) => {
				if(r is null){
					return Task.FromResult(new ReadResult<string>());
				} else{
					bool exist = r.TryGetValue(key, out string v);
					return Task.FromResult(new ReadResult<string>(v, exist));
				}
			});
		}

		public Task<bool> TryAdd(string key, string value)
		{
			return Borrow(async (IDictionary<string, string> r) => {
				if(r.TryAdd(key, value)){
					return true;
				} else{
					return false;
				}
			});
		}

		public Task<bool> TryRemove(string key)
		{
			return Borrow(async (IDictionary<string, string> r) => {
				if(r is null){
					return false;
				}
				if (r.Remove(key))
				{
					return true;
				}
				else
				{
					return false;
				}
			});
		}

		public Task SetOrAdd(string key, string value)
		{
			return Borrow(async (IDictionary<string, string> r) => {
				r[key] = value;
				return false;
			});
		}

		public Task<bool> ForEach(Func<string, string, Task<bool>> func, bool multithreaded)
		{
			return Borrow(async (IDictionary<string, string> r) => {
				if(multithreaded){
					Queue<Task<bool>> tasks = new Queue<Task<bool>>();
					MultithreadedAsyncIterationHelper stopper = new MultithreadedAsyncIterationHelper();
					foreach (KeyValuePair<string, string> kvp in r)
					{
						Task<bool> tsk = func(kvp.Key, kvp.Value);
						if(stopper.Stopped){
							await tsk;
							while(tasks.TryDequeue(out tsk)){
								await tsk;
							}
							return false;
						} else{
							stopper.TryStop(tsk);
							tasks.Enqueue(tsk);
						}
					}
					bool ret = true;
					while (tasks.TryDequeue(out Task<bool> tsk2))
					{
						ret &= await tsk2;
					}
					return ret;
				} else{
					foreach (KeyValuePair<string, string> kvp in r)
					{
						if (!await func(kvp.Key, kvp.Value))
						{
							return false;
						}
					}
					return true;
				}
				
			});
		}
	}
	public sealed class MultithreadedAsyncIterationHelper{
		private volatile bool stopped;
		public bool Stopped => stopped;

		public async void TryStop(Task<bool> tsk){
			if(!await tsk){
				stopped = true;
			}
		}
	}

	/// <summary>
	/// A sharded cached dictionary
	/// </summary>
	public sealed class ShardedAsyncDictionary : IAsyncDictionary{
		
		private readonly IAsyncDictionary[] buckets;
		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();
		private readonly Func<IAsyncDictionary> factory;
		public ShardedAsyncDictionary(int bucketsCount, Func<IAsyncDictionary> factory){
			if(bucketsCount < 1){
				throw new InvalidOperationException("Sharded async dictionary needs at least 1 bucket");
			}
			buckets = new IAsyncDictionary[bucketsCount];
			this.factory = factory;
		}

		private int GetBucketIndex(string key){
			int hash = ("LesbiansAreCute" + key).GetHashCode();
			if(hash < 0){
				hash = 1 - hash;
			}
			return hash % buckets.Length;
		}

		public async Task SetOrAdd(string key, string value)
		{
			await asyncReaderWriterLock.AcquireWriterLock();
			try
			{
				int index = GetBucketIndex(key);
				IAsyncDictionary asyncDictionary = buckets[index];
				if (asyncDictionary is null)
				{
					asyncDictionary = factory();
					buckets[index] = asyncDictionary;
					await asyncDictionary.TryAdd(key, value);
				} else{
					await asyncDictionary.SetOrAdd(key, value);
				}
			}
			finally
			{
				asyncReaderWriterLock.ReleaseWriterLock();
			}
		}

		public async Task<bool> TryAdd(string key, string value)
		{
			await asyncReaderWriterLock.AcquireWriterLock();
			try
			{
				int index = GetBucketIndex(key);
				IAsyncDictionary asyncDictionary = buckets[index];
				if (asyncDictionary is null)
				{
					asyncDictionary = factory();
					buckets[index] = asyncDictionary;
				}
				return await asyncDictionary.TryAdd(key, value);
			}
			finally
			{
				asyncReaderWriterLock.ReleaseWriterLock();
			}
		}

		public async Task<ReadResult<string>> TryGetValue(string key)
		{
			await asyncReaderWriterLock.AcquireReaderLock();
			try{
				IAsyncDictionary asyncDictionary = buckets[GetBucketIndex(key)];
				if(asyncDictionary is null){
					return new ReadResult<string>();
				} else{
					return await asyncDictionary.TryGetValue(key);
				}
			} finally{
				asyncReaderWriterLock.ReleaseReaderLock();
			}
		}

		public async Task<bool> TryRemove(string key)
		{
			await asyncReaderWriterLock.AcquireWriterLock();
			try{
				IAsyncDictionary asyncDictionary = buckets[GetBucketIndex(key)];
				if(asyncDictionary is null){
					return false;
				} else{
					return await asyncDictionary.TryRemove(key);
				}
			} finally{
				asyncReaderWriterLock.ReleaseWriterLock();
			}
		}

		public async Task<bool> ForEach(Func<string, string, Task<bool>> func, bool multithreaded)
		{
			await asyncReaderWriterLock.AcquireReaderLock();
			try{
				if(multithreaded)
				{
					MultithreadedAsyncIterationHelper stopper = new MultithreadedAsyncIterationHelper();
					Queue<Task<bool>> tasks = new Queue<Task<bool>>();
					foreach (IAsyncDictionary asyncDictionary in buckets)
					{
						if (asyncDictionary is { })
						{
							Task<bool> tsk = asyncDictionary.ForEach(func, true);
							stopper.TryStop(tsk);
							tasks.Enqueue(tsk);
						}
						if(stopper.Stopped){
							while (tasks.TryDequeue(out Task <bool> tsk))
							{
								await tsk;
							}
							return false;
						}
					}
					bool ret = true;
					while (tasks.TryDequeue(out Task <bool> tsk)){
						ret &= await tsk;
					}
					return ret;
				} else{
					foreach (IAsyncDictionary asyncDictionary in buckets)
					{
						if (asyncDictionary is { })
						{
							if (!await asyncDictionary.ForEach(func, false))
							{
								return false;
							}
						}
					}
				}
				
			} finally{
				asyncReaderWriterLock.ReleaseReaderLock();
			}
			return true;
		}
	}
	public sealed class InMemoryAsyncDictionary : IAsyncDictionary
	{
		private readonly AsyncReaderWriterLock locker = new AsyncReaderWriterLock();
		private readonly ConcurrentDictionary<string, string> keyValuePairs = new ConcurrentDictionary<string, string>();
		public async Task<bool> ForEach(Func<string, string, Task<bool>> func, bool multithreaded)
		{
			await locker.AcquireWriterLock();
			try
			{
				if(multithreaded){
					Queue<Task<bool>> tasks = new Queue<Task<bool>>();
					MultithreadedAsyncIterationHelper stopper = new MultithreadedAsyncIterationHelper();
					bool net = true;
					foreach (KeyValuePair<string, string> kvp in keyValuePairs.ToArray())
					{
						Task<bool> tsk = func(kvp.Key, kvp.Value);
						if(stopper.Stopped){
							net = false;
							break;
						} else{
							stopper.TryStop(tsk);
							tasks.Enqueue(tsk);
						}
					}
					while(tasks.TryDequeue(out Task<bool> tsk2)){
						net &= await tsk2;
					}
					return net;
				} else{
					foreach (KeyValuePair<string, string> kvp in keyValuePairs.ToArray())
					{
						if(!await func(kvp.Key, kvp.Value)){
							return false;
						}
					}
					return true;
				}
			}
			finally
			{
				locker.ReleaseWriterLock();
			}
		}

		public async Task SetOrAdd(string key, string value)
		{
			await locker.AcquireReaderLock();
			try
			{
				keyValuePairs[key] = value;
			}
			finally
			{
				locker.ReleaseReaderLock();
			}
		}

		public async Task<bool> TryAdd(string key, string value)
		{
			await locker.AcquireReaderLock();
			try
			{
				return keyValuePairs.TryAdd(key, value);
			}
			finally
			{
				locker.ReleaseReaderLock();
			}
		}

		public async Task<ReadResult<string>> TryGetValue(string key)
		{
			await locker.AcquireReaderLock();
			try
			{
				bool exist = keyValuePairs.TryGetValue(key, out string value);
				return new ReadResult<string>(value, exist);
			}
			finally
			{
				locker.ReleaseReaderLock();
			}
		}

		public async Task<bool> TryRemove(string key)
		{
			await locker.AcquireReaderLock();
			try{
				return keyValuePairs.TryRemove(key, out _);
			} finally{
				locker.ReleaseReaderLock();
			}
		}
	}
	public sealed class LRUWriteBackCache : ICachePreloadingAsyncDictionary
	{
		
		
		private readonly ConcurrentDictionary<string, CacheEntry> cacheEntries = new ConcurrentDictionary<string, CacheEntry>();
		private readonly AsyncReaderWriterLock locker = new AsyncReaderWriterLock();
		private readonly IAsyncDictionary underlying;

		public LRUWriteBackCache(IAsyncDictionary underlying, int maxcache)
		{
			this.underlying = underlying ?? throw new ArgumentNullException(nameof(underlying));
			Collect(new WeakReference<LRUWriteBackCache>(this, false), maxcache, cacheEntries, locker, underlying);
		}

		private static long GetTimestamp(KeyValuePair<string, CacheEntry> keyValuePair)
		{
			return keyValuePair.Value.timestampWrapper.Value;
		}

		private static async void Collect(WeakReference<LRUWriteBackCache> weakReference, int maxcache, ConcurrentDictionary<string, CacheEntry> cacheEntries, AsyncReaderWriterLock locker, IAsyncDictionary underlying){
			while(weakReference.TryGetTarget(out _)){
				if (cacheEntries.Count > maxcache){
					await locker.AcquireWriterLock();

					try{
						Queue<Task> tasks = new Queue<Task>();
						foreach(KeyValuePair<string, CacheEntry> keyValuePair in cacheEntries.ToArray().OrderBy(GetTimestamp)){
							
							if(cacheEntries.Count < maxcache){
								break;
							}
						
							string key = keyValuePair.Key;
							if (cacheEntries.TryRemove(key, out CacheEntry cacheEntry))
							{
								if(cacheEntry.dirty){
									tasks.Enqueue(underlying.SetOrAdd(key, cacheEntry.value));
								}
							}
						}

						while(tasks.TryDequeue(out Task tsk)){
							await tsk;
						}
					} finally{
						locker.ReleaseWriterLock();
					}
				}
				
				await Task.Delay(500);
			}
		}

		public async Task<ReadResult<string>> TryGetValue(string key)
		{
			await locker.AcquireReaderLock();
			try{
			start:
				if(cacheEntries.TryGetValue(key, out CacheEntry cacheEntry)){
					cacheEntry.timestampWrapper.Renew();
					return new ReadResult<string>(cacheEntry.value, true);
				} else{
					ReadResult<string> readResult = await underlying.TryGetValue(key);
					if(readResult.exist){
						cacheEntry = new CacheEntry(readResult.res, false);
						if(!cacheEntries.TryAdd(key, cacheEntry)){
							goto start; //Optimistic retry
						}
					}
					return readResult;
					
				}
			} finally{
				locker.ReleaseReaderLock();
			}
		}

		public async Task<bool> TryAdd(string key, string value)
		{
			await locker.AcquireWriterLock();
			try{
				if((await underlying.TryGetValue(key)).exist){
					return false;
				} else{
					return cacheEntries.TryAdd(key, new CacheEntry(value, true));
				}
				
			} finally{
				locker.ReleaseWriterLock();
			}
		}

		public async Task<bool> TryRemove(string key)
		{
			await locker.AcquireWriterLock();
			try
			{
				Task<bool> a = underlying.TryRemove(key);
				if(cacheEntries.TryRemove(key, out _)){
					await a;
					return true;
				} else{
					return await a;
				}
			}
			finally
			{
				locker.ReleaseWriterLock();
			}
		}

		public Task SetOrAdd(string key, string value)
		{
			//No-touch write
			cacheEntries[key] = new CacheEntry(value, true);
			return Misc.completed;
		}

		public async Task<bool> ForEach(Func<string, string, Task<bool>> func, bool multithreaded)
		{
			Dictionary<string, Task<bool>> caught = new Dictionary<string, Task<bool>>();
			
			await locker.AcquireWriterLock();
			try{
				if(multithreaded){
					MultithreadedAsyncIterationHelper stopper = new MultithreadedAsyncIterationHelper();
					foreach (KeyValuePair<string, CacheEntry> kvp in cacheEntries.ToArray())
					{
						string k = kvp.Key;
						Task<bool> tsk = func(k, kvp.Value.value);
						
						if(stopper.Stopped){
							await tsk;
							foreach(Task<bool> tsk2 in caught.Values){
								await tsk2;
							}
							return false;
						} else{
							stopper.TryStop(tsk);
							caught.Add(k, tsk);
						}
						
						
					}

					bool ret = true;
					foreach (Task<bool> tsk2 in caught.Values)
					{
						ret &= await tsk2;
					}
					if (ret)
					{
						Queue<Task<bool>> tasks = new Queue<Task<bool>>();
						return await underlying.ForEach(async (string key, string val) =>
						{
							if (!caught.ContainsKey(key))
							{
								if (!await func(key, val))
								{
									return false;
								}
							}
							return true;
						}, false);
					}
					else{
						return false;
					}
					
				} else{
					foreach (KeyValuePair<string, CacheEntry> kvp in cacheEntries.ToArray())
					{
						string k = kvp.Key;
						caught.Add(k, null);
						if (!await func(k, kvp.Value.value))
						{
							return false;
						}
					}
					return await underlying.ForEach(async (string key, string val) =>
					{
						if (!caught.Remove(key))
						{
							if (!await func(key, val))
							{
								return false;
							}
						}
						return true;
					}, false);
				}
			} finally{
				locker.ReleaseWriterLock();
			}
		}

		public async void Preload(string key)
		{
			await locker.AcquireReaderLock();
			try{
			start:
				if (cacheEntries.TryGetValue(key, out CacheEntry cacheEntry))
				{
					//Renew cache entry
					cacheEntry.timestampWrapper.Renew();
				} else{
					ReadResult<string> readResult = await underlying.TryGetValue(key);
					if (readResult.exist)
					{
						cacheEntry = new CacheEntry(readResult.res, false);
						if (!cacheEntries.TryAdd(key, cacheEntry))
						{
							goto start; //Optimistic retry
						}
					}
				}
			} finally{
				locker.ReleaseReaderLock();
			}
		}

		private readonly struct CacheEntry{
			public sealed class TimestampWrapper{
				private long value;

				public TimestampWrapper(long value)
				{
					this.value = value;
					Interlocked.MemoryBarrier();
				}

				public long Value => Volatile.Read(ref value);

				public void Renew(){
					Volatile.Write(ref value, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
				}
				
			}
			public readonly TimestampWrapper timestampWrapper;
			public readonly string value;
			public readonly bool dirty;

			public CacheEntry(string value, bool dirty)
			{
				this.value = value;
				this.dirty = dirty;
				timestampWrapper = new TimestampWrapper(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
			}
		}
		
	}
}
