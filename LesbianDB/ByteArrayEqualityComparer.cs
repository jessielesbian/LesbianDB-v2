using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Security.Cryptography;
using System.Threading;

namespace LesbianDB
{
	/// <summary>
	/// A singleton equality comparer for byte arrays
	/// </summary>
	public sealed class ByteArrayEqualityComparer : IEqualityComparer<ReadOnlyMemory<byte>>
	{
		public static readonly IEqualityComparer<ReadOnlyMemory<byte>> instance = new ByteArrayEqualityComparer();
		private readonly Func<ReadOnlyMemory<byte>, int> GHCIMPL;
		private ByteArrayEqualityComparer(){
			ReadOnlyMemory<byte> blinder;

			//The blinding seed protects us against certain attacks
			using(RandomNumberGenerator rng = RandomNumberGenerator.Create()){
				byte[] blinder2 = new byte[48];
				rng.GetBytes(blinder2, 0, 48);
				blinder = blinder2;
			}

			Interlocked.MemoryBarrier();

			//lambda-based isolation of high-security hashing randomizer

			GHCIMPL = (ReadOnlyMemory<byte> bytes) => {
				Span<byte> hash = stackalloc byte[48];

				using (SHA384 sha384 = SHA384.Create())
				{
					if (!sha384.TryComputeHash(bytes.Span, hash, out _)){
						throw new CryptographicException("Unable to pre-hash byte array");
					}
				}
				{
					ReadOnlySpan<byte> shadow = blinder.Span;
					int i = 0;
					while (i < 48)
					{
						hash[i] ^= shadow[i];
						i += 1;
					}
				}
				Span<byte> minihash = stackalloc byte[32];
				using(SHA256 sha256 = SHA256.Create()){
					if(!sha256.TryComputeHash(hash, minihash, out _)){
						throw new CryptographicException("Unable to post-hash byte array");
					}
				}
				return BitConverter.ToInt32(minihash.Slice(0, 4));
			};
		}
		public bool Equals(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
		{
			int length = x.Length;
			if(x.Equals(y)){
				return true;
			} else if(length == y.Length){
				ReadOnlySpan<byte> xs = x.Span;
				ReadOnlySpan<byte> ys = y.Span;
				int i = 0;
				while(i < length){
					if(xs[i].Equals(ys[i])){
						++i;
					} else{
						return false;
					}
				}
				return true;
			} else{
				return false;
			}
		}

		public int GetHashCode(ReadOnlyMemory<byte> bytes) => GHCIMPL(bytes);
	}
}
