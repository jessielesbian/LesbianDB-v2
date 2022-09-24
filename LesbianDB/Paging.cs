using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System.IO.MemoryMappedFiles;
using System.Buffers;
using System.Security.Cryptography;

namespace LesbianDB
{
	/// <summary>
	/// LesbianDB-on-flash paging
	/// </summary>
	public static class PagedMemoryPool {
		static PagedMemoryPool() {
			ReadOnlyMemory<byte> readOnlyMemory = new byte[0];
			empty = () => readOnlyMemory;

		}
		private static readonly ConcurrentBag<WeakReference<Arena>> fastarenas = new ConcurrentBag<WeakReference<Arena>>();
		private static readonly ConcurrentBag<WeakReference<Arena>> slowarenas = new ConcurrentBag<WeakReference<Arena>>();
		private static readonly Func<ReadOnlyMemory<byte>> empty;
		private static Arena GetPage() {
		start:
			if (fastarenas.TryTake(out WeakReference<Arena> wr)) {
				if (wr.TryGetTarget(out Arena pageFile)) {
					return pageFile;
				} else {
					goto start;
				}
			}
		start2:
			if (slowarenas.TryTake(out wr))
			{
				if (wr.TryGetTarget(out Arena pageFile))
				{
					return pageFile;
				}
				else
				{
					goto start2;
				}
			}
			else {
				return null;
			}
		}

		private static void DoNothing(Stream str){
			
		}
		public static Action<Stream> Alloc(ReadOnlyMemory<byte> bytes)
		{
			return Alloc(bytes.Span);
		}

		public static Action<Stream> Alloc(ReadOnlySpan<byte> bytes){
			if(bytes.IsEmpty){
				return DoNothing;
			}

			
			Queue<Action<Stream>> blocks = new Queue<Action<Stream>>();
			Queue<WeakReference<Arena>> readd = new Queue<WeakReference<Arena>>();
			int il = bytes.Length;
			try
			{
				int len = bytes.Length;
				while (len > 0)
				{
					if(len < 16){
						ReadOnlyMemory<byte> tmpbytes = bytes.ToArray();
						blocks.Enqueue((Stream str) => str.Write(tmpbytes.Span));
						break;
					}
					Arena pageFile = null;
					try
					{
						pageFile = GetPage();

						if(pageFile is null){
							if(len > 16777216){
								//If we have more than 4096 blocks to go, and we don't have partially used arenas to fill, then we resort to allocating a large file instead of allocating a new arena.
								//We want to minimize the allocation of new arenas, or the destruction of existing ones.
								MemoryMappedFile largefile = MemoryMappedFile.CreateNew(null, len);
								using (MemoryMappedViewStream view = largefile.CreateViewStream(0, len, MemoryMappedFileAccess.Write)){
									view.Write(bytes);
								}
								blocks.Enqueue(new SimpleBuffer(largefile, len).CopyTo);
								break;
							} else{
								pageFile = new Arena();
							}
						}

						while(true){
							
							if(len > 4096){

								PageBuffer blk = pageFile.Alloc(bytes.Slice(0, 4096));
								if (blk is null){
									break;
								} else{
									blocks.Enqueue(blk.Copy);
									bytes = bytes[4096..];
									len -= 4096;
								}
							} else{
								PageBuffer blk = pageFile.Alloc(bytes);
								if (blk is { }){
									blocks.Enqueue(blk.Copy);
									len = 0;
								}
								break;
							}
						}
					}
					finally
					{
						if (pageFile is { })
						{
							readd.Enqueue(new WeakReference<Arena>(pageFile, false));
						}
					}
				}
			} finally{
				while(readd.TryDequeue(out WeakReference<Arena> wr)){
					if(wr.TryGetTarget(out Arena arena)){
						if(arena.Slow){
							slowarenas.Add(wr);
						} else{
							fastarenas.Add(wr);
						}
					}
				}
			}
			if(blocks.Count == 1){
				return blocks.Dequeue();
			} else{
				ReadOnlyMemory<Action<Stream>> functions = blocks.ToArray();
				return (Stream str) => {
					foreach(Action<Stream> func in functions.Span){
						func(str);
					}
				};
			}
			
		}
		
		private sealed class Arena{
			private readonly MemoryMappedFile memoryMappedFile = MemoryMappedFile.CreateNew(null, 16777216, MemoryMappedFileAccess.ReadWrite, MemoryMappedFileOptions.None, HandleInheritability.None);
			private readonly ConcurrentBag<short> blocks = new ConcurrentBag<short>();

			//We use a "free pointer" for extremely fast initial allocation
			private int freeptr;

			public bool Slow => freeptr == 4096;

			public Arena(){
				GC.SuppressFinalize(memoryMappedFile);
			}

			public PageBuffer Alloc(ReadOnlySpan<byte> bytes){
				short offset;
				
				if(freeptr < 4096){
					//Note: this is thread-safe, since the arena is removed from the pool during the operation
					offset = (short)freeptr++;
				} else if(blocks.Count == 4096){
					//[Optimization] If all blocks are free, we can reset free pointer and clear free list
					offset = 0;
					freeptr = 0;
					blocks.Clear();
				} else if(!blocks.TryTake(out offset)){
					return null;
				}
				int off2 = offset * 4096;
				int len = bytes.Length;
				MemoryMappedViewAccessor str = null;
				byte[] tmpbuf = null;
				try
				{
					str = memoryMappedFile.CreateViewAccessor(off2, len, MemoryMappedFileAccess.ReadWrite);
					tmpbuf = RentBuffer();
					bytes.CopyTo(tmpbuf);
					str.WriteArray(0, tmpbuf, 0, len);
					return new PageBuffer(() => {
						try
						{
							blocks.Add(offset);
						}
						catch (ObjectDisposedException)
						{
							//Do nothing
						}
					}, str, len);
				}
				catch
				{
					if (tmpbuf is { }){
						arp.Add(tmpbuf);
					}
					blocks.Add(offset);
					str?.Dispose();
					throw;
				}
			}
			
			~Arena(){
				memoryMappedFile.Dispose();
			}
		}
		private static readonly ConcurrentBag<byte[]> arp = new ConcurrentBag<byte[]>();
		private static byte[] RentBuffer(){
			if(arp.TryTake(out byte[] bytes)){
				return bytes;
			} else{
				return new byte[4096];
			}
		}
		private sealed class PageBuffer
		{
			private readonly Action freeThis;
			private readonly MemoryMappedViewAccessor view;
			private readonly int len;

			public void Copy(Stream str){
				byte[] tmpbuf = null;
				try{
					tmpbuf = RentBuffer();
					view.ReadArray(0, tmpbuf, 0, len);
					str.Write(tmpbuf, 0, len);
				} finally{
					if(tmpbuf is { }){
						arp.Add(tmpbuf);
					}
				}
			}

			public PageBuffer(Action free2, MemoryMappedViewAccessor stream, int len)
			{
				freeThis = free2;
				this.view = stream;
				this.len = len;
			}

			~PageBuffer()
			{
				view.Dispose();
				freeThis();
			}
		}

		private sealed class SimpleBuffer{
			private readonly MemoryMappedFile memoryMappedFile;
			private readonly int len;
			public SimpleBuffer(MemoryMappedFile mf, int len){
				GC.SuppressFinalize(mf);
				memoryMappedFile = mf;
				this.len = len;
			}

			public void CopyTo(Stream stream){
				using Stream view = memoryMappedFile.CreateViewStream(0, len, MemoryMappedFileAccess.Read);
				view.CopyTo(stream);
			}
			~SimpleBuffer(){
				memoryMappedFile.Dispose();
			}
		}
	}

	/// <summary>
	/// LesbianDB-on-HDD paging engine (note use of async file streams instead of memory mapped files)
	/// </summary>
	public static class HDDPagedMemoryPool{
		private static readonly ConcurrentBag<Stream> streams = new ConcurrentBag<Stream>();
		public static async Task<Func<Stream, Task>> Alloc(Stream source){
			Func<Stream, Task> func = new HDDPagedMemory(source, out Task init).Read;
			await init;
			return func;
		}
		private sealed class HDDPagedMemory{
			private readonly Stream stream;
			private readonly AsyncMutex asyncMutex = new AsyncMutex();
			public HDDPagedMemory(Stream source, out Task init){
				if (!streams.TryTake(out Stream str))
				{
					str = new FileStream(Misc.GetRandFileName(), FileMode.Create, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose | FileOptions.DeleteOnClose | FileOptions.Asynchronous);
					GC.SuppressFinalize(str);
				}
				stream = str;
				init = Init(source);
			}

			private async Task Init(Stream source)
			{
				await source.CopyToAsync(stream);
				await stream.FlushAsync();
				stream.Seek(0, SeekOrigin.Begin);
			}
			
			public async Task Read(Stream str){
				await asyncMutex.Enter();
				try{
					stream.Seek(0, SeekOrigin.Begin);
					await stream.CopyToAsync(str);
				} finally{
					asyncMutex.Exit();
				}
			}

			~HDDPagedMemory(){
				stream.SetLength(0);
				streams.Add(stream);
			}
		}
	}
}
