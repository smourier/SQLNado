using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlNado.Utilities;

namespace SqlNado.Tests
{
    [TestClass]
    public class Database
    {
        [TestMethod]
        public void CreateDatabase()
        {
            using (var db = new SQLiteDatabase(":memory:"))
            {
                db.Save(new Customer1());

                var table = db.GetTable("Customer");
                Assert.AreEqual(2, table.Columns.Count);
                Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);
            }
        }

        [TestMethod]
        public void CreateIndices()
        {
            using (var db = new SQLiteDatabase(":memory:"))
            {
                db.EnableStatementsCache = true;
                db.Save(new Customer4());

                var table = db.GetTable<Customer4>();

                Assert.AreEqual(3, table.Columns.Count);
                Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("FirstName").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("LastName").Type);

                var indices = table.Indices.ToList();
                Assert.AreEqual(1, indices.Count);
                var cols = indices[0].IndexColumns.ToList();
                Assert.AreEqual(3, cols.Count);
                Assert.AreEqual("FirstName", cols[0].Name);
                Assert.AreEqual("LastName", cols[1].Name);
                Assert.AreEqual(true, cols[2].IsRowId);
            }
        }

        [TestMethod]
        public void SchemaMigration_PreserveUnusedColumns()
        {
            using (var db = new SQLiteDatabase(":memory:"))
            {
                db.SynchronizeSchema<Customer1>();
                var table = db.GetTable("Customer");
                Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);

                db.SynchronizeSchema<Customer2>();
                table = db.GetTable("Customer");
                Assert.AreEqual(4, table.Columns.Count);
                Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("FirstName").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("LastName").Type);
            }
        }

        [TestMethod]
        public void SchemaMigration_DeleteUnusedColumns()
        {
            using (var db = new SQLiteDatabase(":memory:"))
            {
                db.SynchronizeSchema<Customer1>();
                var table = db.GetTable("Customer");
                Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);

                var saveOptions = db.CreateSaveOptions();
                if (saveOptions == null)
                    throw new InvalidOperationException();

                saveOptions.DeleteUnusedColumns = true;
                db.SynchronizeSchema<Customer2>(saveOptions);
                table = db.GetTable("Customer");
                Assert.AreEqual(3, table.Columns.Count);
                Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("FirstName").Type);
                Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("LastName").Type);
            }
        }

        [TestMethod]
        public void SchemaMigration_PreserveValues()
        {
            using (var db = new SQLiteDatabase(":memory:"))
            {
                db.Save(new Customer1 { Id = 1, Name = "Row1" });
                db.SynchronizeSchema<Customer3>();

                var customer = db.LoadAll<Customer3>().Single();
                Assert.AreEqual("1", customer.Id);
                Assert.AreEqual("Row1", customer.Name);
            }
        }

        [TestMethod]
        public void TestAllTypes()
        {
            using (var db = new SQLiteDatabase(":memory:"))
            {
                var at = new AllTypes();
                at.Boolean = true;
                at.Byte = 128;
                at.DateTime = DateTime.Now;
                at.DateTimeOffset = DateTimeOffset.Now;
                at.Decimal = 10230120310230102301230m;
                at.Double = 102103.1923291;
                at.Guid = Guid.NewGuid();
                at.Int16 = -12345;
                at.Int32 = -12345678;
                at.Int64 = -12345678900123;
                at.MyEnum = MyEnum.Second;
                at.MyFlagsEnum = MyFlagsEnum.Four | MyFlagsEnum.One;
                at.Name = "Bob" + Environment.TickCount;
                at.SByte = -123;
                at.Single = 101023.131F;
                at.TimeSpan = new TimeSpan(1, 2, 3, 4, 5);
                at.UInt16 = 50021;
                at.UInt32 = 1030120312;
                at.UInt64 = 10310230094912131;
                db.Save(at);

                var at2 = db.LoadAll<AllTypes>().First();
                Assert.AreEqual(at.Boolean, at2.Boolean);
                Assert.AreEqual(at.Byte, at2.Byte);
                Assert.AreEqual(at.DateTime, at2.DateTime);
                Assert.AreEqual(at.DateTimeOffset, at2.DateTimeOffset);
                Assert.AreEqual(at.Decimal, at2.Decimal);
                Assert.AreEqual(at.Double, at2.Double);
                Assert.AreEqual(at.Guid, at2.Guid);
                Assert.AreEqual(at.Int16, at2.Int16);
                Assert.AreEqual(at.Int32, at2.Int32);
                Assert.AreEqual(at.Int64, at2.Int64);
                Assert.AreEqual(at.MyEnum, at2.MyEnum);
                Assert.AreEqual(at.MyFlagsEnum, at2.MyFlagsEnum);
                Assert.AreEqual(at.Name, at2.Name);
                Assert.AreEqual(at.SByte, at2.SByte);
                Assert.AreEqual(at.Single, at2.Single);
                Assert.AreEqual(at.TimeSpan, at2.TimeSpan);
                Assert.AreEqual(at.UInt16, at2.UInt16);
                Assert.AreEqual(at.UInt32, at2.UInt32);
                Assert.AreEqual(at.UInt64, at2.UInt64);
            }
        }

        [SQLiteTable(Name = "Customer")]
        private class Customer1
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        [SQLiteTable(Name = "Customer")]
        private class Customer2
        {
            public int Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        [SQLiteTable(Name = "Customer")]
        private class Customer3
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [SQLiteTable(Name = "Customer")]
        private class Customer4
        {
            [SQLiteColumn(IsPrimaryKey = true, Name = "Id")]
            public long Identifier { get; set; }

            [SQLiteIndex("NameIndex")]
            public string FirstName { get; set; }

            [SQLiteIndex("NameIndex")]
            public string LastName { get; set; }
        }

        [SQLiteTable(Name = "Customer")]
        private class AllTypes
        {
            public string Name { get; set; }
            public DateTime DateTime { get; set; }
            public DateTimeOffset DateTimeOffset { get; set; }
            public Guid Guid { get; set; }
            public TimeSpan TimeSpan { get; set; }
            public decimal Decimal { get; set; }
            public float Single { get; set; }
            public double Double { get; set; }
            public bool Boolean { get; set; }
            public long Int64 { get; set; }
            public int Int32 { get; set; }
            public short Int16 { get; set; }
            public sbyte SByte { get; set; }
            public ulong UInt64 { get; set; }
            public uint UInt32 { get; set; }
            public ushort UInt16 { get; set; }
            public byte Byte { get; set; }
            public MyEnum MyEnum { get; set; }
            public MyFlagsEnum MyFlagsEnum { get; set; }
        }

        private enum MyEnum
        {
            First,
            Second,
            Third,
        }

        [Flags]
        private enum MyFlagsEnum
        {
            None = 0x0,
            One = 0x1,
            Two = 0x2,
            Four = 0x4,
            Eight = 0x8,
        }
    }
}
