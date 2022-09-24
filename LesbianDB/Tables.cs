using System;
using System.Collections.Generic;
using System.Text;
using CodeExMachina;

namespace LesbianDB
{
	//WORK IN PROGRESS
	public enum RowType : byte{
		Data = 0, Unique = 1, Sorted = 2
	}
	public readonly struct ColumnDescriptor{
		public readonly string name;
		public readonly RowType rowType;
	}

	public sealed class Table{
		private readonly struct Row{
			public readonly RowType columnType;

			//Used for sorted columns
			public readonly BTree<string> bTree;

			//Used for unique and sorted columns
			public readonly IAsyncDictionary map;

			public Row(RowType c, Func<IAsyncDictionary> func){
				
				if(c > 0){
					map = func();
					if(c == RowType.Sorted){
						bTree = new BTree<string>(2, DefaultComparer<string>.instance);
						
					} else{
						bTree = null;
					}
				} else{
					map = null;
					bTree = null;
				}
				columnType = c;
			}
		}

		private readonly AsyncReaderWriterLock asyncReaderWriterLock = new AsyncReaderWriterLock();
		private readonly Dictionary<string, Row> rows = new Dictionary<string, Row>();

		public Table(Span<ColumnDescriptor> columnDescriptors){
			
		}
	}
}
