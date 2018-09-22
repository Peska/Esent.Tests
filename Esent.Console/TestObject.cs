using System;

namespace Esent.Console
{
	[Serializable]
	public struct TestObject
	{
		public string EQNum { get; set; }
		public string Name { get; set; }
		public string IDNumber { get; set; }
		public string FirstName { get; set; }
		public string LastName { get; set; }
		public string Pesel { get; set; }
		public string ReferenceNumber { get; set; }
		public string CompanyName { get; set; }
		public string Mnemonic { get; set; }

		public static TestObject Create()
		{
			return new TestObject
			{
				EQNum = Guid.NewGuid().ToString(),
				Name = Guid.NewGuid().ToString(),
				IDNumber = Guid.NewGuid().ToString().Substring(0, 9),
				FirstName = Guid.NewGuid().ToString().Substring(0, 10),
				LastName = Guid.NewGuid().ToString().Substring(0, 15),
				Pesel = Guid.NewGuid().ToString().Substring(0, 11),
				ReferenceNumber = Guid.NewGuid().ToString().Substring(0, 13),
				CompanyName = Guid.NewGuid().ToString(),
				Mnemonic = Guid.NewGuid().ToString().Substring(0, 9)
			};
		}
	}
}
