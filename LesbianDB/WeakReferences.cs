using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.Serialization;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using System.Threading;
using System.Security.Cryptography;
using System.Runtime.Serialization.Formatters.Binary;

namespace LesbianDB
{
	/// <summary>
	/// A strong reference that is converted to a weak reference after a set amount of time
	/// </summary>
	public sealed class SoftReference<T> where T : class
	{
		private volatile CancellationTokenSource delayCancellationSource;
		private readonly int expiry;
		private long weakeners;
		public SoftReference(T obj, int time)
		{
			if (time < 0)
			{
				throw new ArgumentOutOfRangeException("Negative soft reference lifetime");
			}
			t = obj ?? throw new ArgumentNullException(nameof(obj));
			wr = new WeakReference<T>(obj);
			expiry = time;
			TryRenew();
		}

		/// <summary>
		/// Attempts to renew the soft reference
		/// </summary>
		public async void TryRenew()
		{
			//Optimistically assume that we won't get renewed
			long id = Interlocked.Increment(ref weakeners);

			//Renew-after-expiry
			if (t is null)
			{
				if (wr.TryGetTarget(out T temp))
				{
					t = temp; //We can be reinstated
				}
				else
				{
					return; //We are already dead
				}
			}
			CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
			Interlocked.Exchange(ref delayCancellationSource, cancellationTokenSource)?.Cancel(false);

			try{
				await Task.Delay(expiry, delayCancellationSource.Token);
			} catch{
				//We may have been cancelled
				return;
			}

			//Check if we have been renewed
			if (id == Interlocked.Read(ref weakeners))
			{
				t = null;
			}
		}

		public bool TryGetTarget(out T res)
		{
			res = t;
			return !(res is null) || wr.TryGetTarget(out res);
		}

		private readonly WeakReference<T> wr;
		private volatile T t;
	}
}