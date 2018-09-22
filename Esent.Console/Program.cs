using Microsoft.Database.Isam.Config;
using Microsoft.Isam.Esent.Collections.Generic;
using Microsoft.Isam.Esent.Interop;
using Newtonsoft.Json;
using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Esent.Console
{
	using Console = System.Console;

	class Program
	{
		static void Main(string[] args)
		{
			// PersistentDirectory();

			// RocksDb1();

			// RocksDb2();

			// RocksDb3();

			RocksDb4();
		}

		private static void RocksDb4()
		{
			string directory = @"C:\Users\Peska\source\repos\Esent.Tests\RocksDB";

			DbOptions options = new DbOptions().SetCreateIfMissing(true);

			//SliceTransform sliceTransform = SliceTransform.CreateFixedPrefix(3);
			//ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions().SetPrefixExtractor(sliceTransform);
			//ColumnFamilies columnFamilies = new ColumnFamilies(columnFamilyOptions);

			using (RocksDb rocksDb = RocksDb.Open(options, directory))
			{
				rocksDb.Put("AABCE", "Test 3");
				rocksDb.Put("AAAAB", "Test 1");
				rocksDb.Put("AAABB", "Test 2");
				rocksDb.Put("AABCF", "Test 3");
				rocksDb.Put("AABBB", "Test 3");
				rocksDb.Put("ABBBB", "Test 4");
				rocksDb.Put("AABCC", "Test 3");
				rocksDb.Put("BBBBB", "Test 5");
				rocksDb.Put("AABCC", "Test 3");

				List<string> keys = new List<string>();

				using (Iterator iterator = rocksDb.NewIterator())
				{
					iterator.Seek("AAB");

					while (iterator.Valid())
					{
						keys.Add(iterator.StringKey());

						iterator.Next();
					}
				}

				Console.WriteLine(string.Join(", ", keys));
				Console.ReadLine();
			}
		}

		private static void RocksDb3()
		{
			string directory = @"C:\Users\Peska\source\repos\Esent.Tests\RocksDB";

			var bbto = new BlockBasedTableOptions()
					 .SetFilterPolicy(BloomFilterPolicy.Create(10, false))
					 .SetWholeKeyFiltering(false);

			var options = new DbOptions()
				 .SetCreateIfMissing(true)
				 .SetCreateMissingColumnFamilies(true);

			var columnFamilies = new ColumnFamilies
				{
					 { "default", new ColumnFamilyOptions().OptimizeForPointLookup(256) },
					 { "test", new ColumnFamilyOptions()
                    //.SetWriteBufferSize(writeBufferSize)
                    //.SetMaxWriteBufferNumber(maxWriteBufferNumber)
                    //.SetMinWriteBufferNumberToMerge(minWriteBufferNumberToMerge)
                    .SetMemtableHugePageSize(2 * 1024 * 1024)
						  .SetPrefixExtractor(SliceTransform.CreateFixedPrefix(8))
						  .SetBlockBasedTableFactory(bbto)
					 },
				};

			using (var db = RocksDbSharp.RocksDb.Open(options, directory, columnFamilies))
			{
				var cf = db.GetColumnFamily("test");

				db.Put("00000000Zero", "", cf: cf);
				db.Put("00000001Red", "", cf: cf);
				db.Put("00000000One", "", cf: cf);
				db.Put("00000000Two", "", cf: cf);
				db.Put("00000000Three", "", cf: cf);
				db.Put("00000001Black", "", cf: cf);
				db.Put("00000002Apple", "", cf: cf);
				db.Put("00000002Cranberry", "", cf: cf);
				db.Put("00000001Green", "", cf: cf);
				db.Put("00000002Banana", "", cf: cf);

				var readOptions = new ReadOptions().SetIterateUpperBound("00000002");

				var iter = db.NewIterator(readOptions: readOptions, cf: cf);
				GC.Collect();
				GC.WaitForPendingFinalizers();

				var b = Encoding.UTF8.GetBytes("00000001");

				iter = iter.Seek(b);

				while (iter.Valid())
				{
					Console.WriteLine(iter.StringKey());
					iter = iter.Next();
				}

				Console.ReadKey();
			}
		}

		private static void RocksDb2()
		{
			string directory = @"C:\Users\Peska\source\repos\Esent.Tests\RocksDB";

			DbOptions options = new DbOptions().SetCreateIfMissing();

			using (RocksDb rocksDb = RocksDbSharp.RocksDb.Open(options, directory))
			{
				TestObject o1 = new TestObject { EQNum = "3L1234", Mnemonic = "20013L1234011" };
				TestObject o2 = new TestObject { EQNum = "3L5678", Mnemonic = "20023L5678011" };
				TestObject o3 = new TestObject { EQNum = "3L9012", Mnemonic = "20033L9012011" };
				TestObject o4 = new TestObject { EQNum = "3L9012", Mnemonic = "20013L9012012" };

				rocksDb.Put(o1.Mnemonic, JsonConvert.SerializeObject(o1));
				rocksDb.Put(o2.Mnemonic, JsonConvert.SerializeObject(o2));
				rocksDb.Put(o3.Mnemonic, JsonConvert.SerializeObject(o3));
				rocksDb.Put(o4.Mnemonic, JsonConvert.SerializeObject(o4));

				SliceTransform sliceTransform = SliceTransform.CreateFixedPrefix(4);
				BlockBasedTableOptions blockBasedTableOptions = new BlockBasedTableOptions().SetWholeKeyFiltering(false);

				ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
																				.SetPrefixExtractor(sliceTransform)
																				.SetBlockBasedTableFactory(blockBasedTableOptions);

				ColumnFamilies columnFamilies = new ColumnFamilies(columnFamilyOptions);

				Iterator iterator = rocksDb.NewIterator();
				iterator = iterator.Seek("2001");

				while (iterator.Valid())
				{
					string key = iterator.StringKey();
					string value = iterator.StringValue();

					iterator.Next();
				}
			}
		}

		private static void RocksDb1()
		{
			string directory = @"C:\Users\Peska\source\repos\Esent.Tests\RocksDB";
			int maxSize = 1_000_000;

			TestObject[] objects = new TestObject[maxSize];

			for (int i = 0; i < maxSize; i++)
			{
				objects[i] = TestObject.Create();

				if (i % 1000 == 0)
					Console.WriteLine($"Created {i} records");
			}

			DbOptions options = new DbOptions().SetCreateIfMissing();

			using (RocksDb rocksDb = RocksDbSharp.RocksDb.Open(options, directory))
			{
				for (int i = 0; i < maxSize; i++)
				{
					string json = JsonConvert.SerializeObject(objects[i]);

					rocksDb.Put(objects[i].EQNum, json);

					if (i % 1000 == 0)
						Console.WriteLine($"Indexed {i} records");
				}

				Random random = new Random();

				for (int i = 0; i < maxSize; i++)
				{
					int randomInt = random.Next(maxSize - 1);

					TestObject testObject = objects[randomInt];
					TestObject fromDict = JsonConvert.DeserializeObject<TestObject>(rocksDb.Get(testObject.EQNum));

					if (testObject.EQNum != fromDict.EQNum)
						throw new Exception("What the fuck");

					if (i % 1000 == 0)
						Console.WriteLine($"Retrieved {i} records");
				}

				Iterator iterator = rocksDb.NewIterator();
			}
		}

		private static void PersistentDirectory()
		{
			string directory = @"C:\Users\Peska\source\repos\Esent.Tests\PersistentDictionary\";

			if (PersistentDictionaryFile.Exists(directory))
				PersistentDictionaryFile.DeleteFiles(directory);

			TestObject[] objects = new TestObject[100_000];

			for (int i = 0; i < 100_000; i++)
			{
				objects[i] = TestObject.Create();

				if (i % 1000 == 0)
					Console.WriteLine($"Created {i} records");
			}

			DatabaseConfig config = new DatabaseConfig
			{
				EventLoggingLevel = EventLoggingLevels.Disable,
				CircularLog = true
			};

			config.SetGlobalParams();

			using (PersistentDictionary<int, TestObject> dictionary = new PersistentDictionary<int, TestObject>(directory, config))
			{
				for (int i = 0; i < 100_000; i++)
				{
					dictionary.Add(i, objects[i]);

					if (i % 1000 == 0)
						Console.WriteLine($"Indexed {i} records");
				}

				dictionary.Flush();

				Random random = new Random();

				for (int i = 0; i < 100_000; i++)
				{
					int randomInt = random.Next(100_000 - 1);

					if (!dictionary.TryGetValue(randomInt, out TestObject fromDict))
						throw new Exception("What the fuck");

					if (i % 1000 == 0)
						Console.WriteLine($"Retrieved {i} records");
				}
			}
		}
	}
}
