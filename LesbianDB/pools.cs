using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;
using System.Buffers;
using Newtonsoft.Json;

namespace LesbianDB
{
	
	/// <summary>
	/// A pool of reusable dictionaries
	/// </summary>
	public sealed class DictionaryPool<K, V>{
		public Dictionary<K, V> Alloc(){
		start:
			PooledDictionary temp = head;
			if(temp is null){
				return new PooledDictionary(new WeakReference<DictionaryPool<K, V>>(this, false));
			} else{
				if(ReferenceEquals(Interlocked.CompareExchange(ref head, temp.last, temp), temp)){
					temp.last = null;

					//We re-register the pooled dictionary for finalization
					GC.ReRegisterForFinalize(temp);
					return temp;
				} else{
					//Optimistic locking: if we have a race condition while trying to
					//get the top of the queue, we fail and try again.
					goto start;
				}
			}
		}
		
		private volatile PooledDictionary head;
		private sealed class PooledDictionary : Dictionary<K, V>{
			public volatile PooledDictionary last;
			private Action release;

			public PooledDictionary(WeakReference<DictionaryPool<K, V>> owner)
			{
				release = () => {
					//We don't reference owner since the dictionary is allowed to survive longer than the pool
					if(owner.TryGetTarget(out DictionaryPool<K, V> pool)){
						Clear();
						PooledDictionary temp = pool.head;
						last = temp;
						Interlocked.CompareExchange(ref pool.head, this, temp);
					}
				};
			}

			public void Release(){
				GC.SuppressFinalize(this);
				release();
			}
			
			~PooledDictionary(){
				release();
			}
		}
	}


	
}
