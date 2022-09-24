using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace LesbianDB
{
	[TestClass]
	public sealed class DatabaseTests{
		[TestMethod] public async Task TestEphemeralDatabase(){
			Database database = new Database(Misc.SimpleCreate<InMemoryAsyncDictionary>);
			TransactionResult res = await database.Execute(PrivilegeLevel.Writer, EmptyArray<Command>.instance, new Command[] {
				new Command("DSET", new string[] {"lesbians", "jessielesbian.isflying", "true"}),
				new Command("DGET", new string[] {"lesbians", "jessielesbian.isflying"})
			});

			Assert.IsNull(res.failure);
			Assert.AreEqual("OK", res.result[0]);
			Assert.AreEqual("true", res.result[1]);
		}

		[TestMethod]
		public async Task TestPersistentDatabase()
		{
			using Stream binlog = new MemoryStream();
			Database database = new Database(Misc.SimpleCreate<InMemoryAsyncDictionary>, binlog, -1, true);
			await database.Execute(PrivilegeLevel.Writer, EmptyArray<Command>.instance, new Command[] {
				new Command("DSET", new string[] {"lesbians", "jessielesbian.isflying", "true"})
			});

			binlog.Seek(0, SeekOrigin.Begin);
			database = new Database(Misc.SimpleCreate<InMemoryAsyncDictionary>, binlog, -1, true);


			TransactionResult res = await database.Execute(PrivilegeLevel.Writer, EmptyArray<Command>.instance, new Command[] {
				new Command("DGET", new string[] {"lesbians", "jessielesbian.isflying"})
			});
			Assert.IsNull(res.failure);
			Assert.AreEqual("true", res.result[0]);
		}


	}
	[TestClass]
	public sealed class PagingTests
	{
		[TestMethod]
		public void TestAllocator()
		{
			using RandomNumberGenerator rng = RandomNumberGenerator.Create();
			int i = 0;
			while(++i < 257){
				ReadOnlyMemory<byte> mem;
				{
					Span<byte> len = stackalloc byte[2];
					rng.GetBytes(len);
					int truelen = BitConverter.ToUInt16(len) + 4;
					byte[] arr = new byte[truelen];
					rng.GetBytes(arr, 0, truelen);
					mem = arr;
				}
				byte[] mem2;

				using (MemoryStream memstr = new MemoryStream()){
					PagedMemoryPool.Alloc(mem)(memstr);
					mem2 = memstr.ToArray();
				}

				int x = 0;
				foreach (byte b in mem.Span){
					Assert.AreEqual(b, mem2[x++]);
				}
			}
		}

		private readonly struct DualString{
			public readonly string str1;
			public readonly string str2;

			public DualString(string str1, string str2)
			{
				this.str1 = str1;
				this.str2 = str2;
			}
		}
		private DualString GetDualString(){
			Span<byte> bytes = stackalloc byte[64];
			RandomNumberGenerator.Fill(bytes);
			return new DualString(Convert.ToBase64String(bytes.Slice(0, 32)), Convert.ToBase64String(bytes[32..]));
		}

		private async Task TestAsyncDict(IAsyncDictionary asyncDictionary, int rounds){
			Dictionary<string, string> reference = new Dictionary<string, string>();
			string[] keys = new string[rounds];
			for (int i = 0; i < rounds; ){
				DualString dualString = GetDualString();
				keys[i++] = dualString.str1;
				if (RandomNumberGenerator.GetInt32(0, 1) == 0){
					Assert.AreEqual(reference.TryAdd(dualString.str1, dualString.str2), await asyncDictionary.TryAdd(dualString.str1, dualString.str2));
				} else{
					reference[dualString.str1] = dualString.str2;
					await asyncDictionary.SetOrAdd(dualString.str1, dualString.str2);
				}
				if(RandomNumberGenerator.GetInt32(0, 1) == 0)
				{
					//Test paging as well
					GC.Collect();
				}
				Assert.IsFalse(reference.TryAdd(dualString.str1, dualString.str2));
			}
			int ctr = 0;
			await asyncDictionary.ForEach((string k, string v) =>
			{
				Interlocked.Increment(ref ctr);
				Assert.AreEqual(reference[k], v);
				return Misc.completedTrue;
			}, false);
			await asyncDictionary.ForEach((string k, string v) =>
			{
				Interlocked.Increment(ref ctr);
				Assert.AreEqual(reference[k], v);
				return Misc.completedTrue;
			}, true);
			Assert.AreEqual(rounds * 2, ctr);
			foreach (string key in keys){
				ReadResult<string> readResult = await asyncDictionary.TryGetValue(key);
				Assert.IsTrue(readResult.exist);
				Assert.AreEqual(reference[key], readResult.res);
				if(RandomNumberGenerator.GetInt32(0, 1) == 0){
					GC.Collect();
				}
				Assert.IsTrue(await asyncDictionary.TryRemove(key));
				if (RandomNumberGenerator.GetInt32(0, 1) == 0)
				{
					GC.Collect();
				}
				if (RandomNumberGenerator.GetInt32(0, 1) == 0)
				{
					Assert.IsFalse((await asyncDictionary.TryGetValue(key)).exist);
				} else{
					Assert.IsFalse(await asyncDictionary.TryRemove(key));
				}
			}
			await asyncDictionary.ForEach((string k, string v) =>
			{
				Assert.AreEqual(reference[k], v);
				return Misc.completedTrue;
			}, false);
			await asyncDictionary.ForEach((string k, string v) =>
			{
				Assert.AreEqual(reference[k], v);
				return Misc.completedTrue;
			}, true);
		}

		[TestMethod]
		public async Task TestShadowDictionary(){
			await TestAsyncDict(new ShadowDictionary(new SynchronousBinaryPagingEngine(PagedMemoryPool.Alloc)), 256);
		}
		[TestMethod]
		public async Task TestShardedDictionary()
		{
			await TestAsyncDict(new ShardedAsyncDictionary(64, () => new ShadowDictionary(new SynchronousBinaryPagingEngine(PagedMemoryPool.Alloc))), 256);
		}
		

		[TestMethod]
		public async Task TestCachedDictionary()
		{
			await TestAsyncDict(new LRUWriteBackCache(new ShardedAsyncDictionary(16, () => new ShadowDictionary(new SynchronousBinaryPagingEngine(PagedMemoryPool.Alloc))), 64), 256);
		}

	}
}
