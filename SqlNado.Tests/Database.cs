using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlNado.Tests;

[TestClass]
public class Database
{
    [TestMethod]
    public void CreateDatabase()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.Save(new Customer1());

        var table = db.GetTable("Customer");
        Assert.HasCount(2, table.Columns);
        Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);
    }

    [TestMethod]
    public void CreateIndices()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.EnableStatementsCache = true;
        db.Save(new Customer4());

        var table = db.GetTable<Customer4>();

        Assert.HasCount(3, table.Columns);
        Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("FirstName").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("LastName").Type);

        var indices = table.Indices.ToList();
        Assert.HasCount(1, indices);
        var cols = indices[0].IndexColumns.ToList();
        Assert.HasCount(3, cols);
        Assert.AreEqual("FirstName", cols[0].Name);
        Assert.AreEqual("LastName", cols[1].Name);
        Assert.IsTrue(cols[2].IsRowId);
    }

    [TestMethod]
    public void SchemaMigration_PreserveUnusedColumns()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.SynchronizeSchema<Customer1>();
        var table = db.GetTable("Customer");
        Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);

        db.SynchronizeSchema<Customer2>();
        table = db.GetTable("Customer");
        Assert.HasCount(4, table.Columns);
        Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("FirstName").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("LastName").Type);
    }

    [TestMethod]
    public void SchemaMigration_DeleteUnusedColumns()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.SynchronizeSchema<Customer1>();
        var table = db.GetTable("Customer");
        Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);

        var saveOptions = db.CreateSaveOptions() ?? throw new InvalidOperationException();
        saveOptions.DeleteUnusedColumns = true;
        db.SynchronizeSchema<Customer2>(saveOptions);
        table = db.GetTable("Customer");
        Assert.HasCount(3, table.Columns);
        Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("FirstName").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("LastName").Type);
    }

    [TestMethod]
    public void SchemaMigration_PreserveValues()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.Save(new Customer1 { Id = 1, Name = "Row1" });
        db.SynchronizeSchema<Customer3>();

        var customer = db.LoadAll<Customer3>().Single();
        Assert.AreEqual("1", customer.Id);
        Assert.AreEqual("Row1", customer.Name);
    }

    [TestMethod]
    public void TestAllTypes()
    {
        using var db = new SQLiteDatabase(":memory:");
        var at = new AllTypes
        {
            Boolean = true,
            Byte = 128,
            DateTime = DateTime.Now,
            DateTimeOffset = DateTimeOffset.Now,
            Decimal = 10230120310230102301230m,
            Double = 102103.1923291,
            Guid = Guid.NewGuid(),
            Int16 = -12345,
            Int32 = -12345678,
            Int64 = -12345678900123,
            MyEnum = MyEnum.Second,
            MyFlagsEnum = MyFlagsEnum.Four | MyFlagsEnum.One,
            Name = "Bob" + Environment.TickCount,
            SByte = -123,
            Single = 101023.131F,
            TimeSpan = new TimeSpan(1, 2, 3, 4, 5),
            UInt16 = 50021,
            UInt32 = 1030120312,
            UInt64 = 10310230094912131
        };
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

    [TestMethod]
    public void TestDataAnnotations()
    {
        using var db = new AnnotatedDb(":memory:");
        var cust = new AnnotatedCustomer
        {
            Name = "Bob" + Environment.TickCount,
            NotNullableGuid = Guid.NewGuid()
        };
        db.Save(cust);

        var table = db.Tables.First();
        Assert.HasCount(5, table.Columns);
        Assert.AreEqual(nameof(AnnotatedCustomer.Id), table.Columns[0].Name);
        Assert.AreEqual(nameof(AnnotatedCustomer.NullableGuid), table.Columns[1].Name);
        Assert.AreEqual(nameof(AnnotatedCustomer.NotNullableGuid), table.Columns[2].Name);
        Assert.AreEqual(nameof(AnnotatedCustomer.Guid), table.Columns[3].Name);
        Assert.AreEqual("OtherName", table.Columns[4].Name);

        var rows = table.GetRows().ToArray();

        var cust2 = db.LoadByPrimaryKey<AnnotatedCustomer>(cust.Id);
        Assert.AreEqual(cust.Guid, cust2.Guid);
        Assert.AreEqual(cust.Id, cust2.Id);
        Assert.AreEqual(cust.Name, cust2.Name);
        Assert.AreEqual(cust.NotNullableGuid, cust2.NotNullableGuid);
        Assert.AreEqual(cust.NullableGuid, cust2.NullableGuid);
    }

    [SQLiteTable(Name = "Customer")]
    private sealed class Customer1
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    [SQLiteTable(Name = "Customer")]
    private sealed class Customer2
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

    [SQLiteTable(Name = "Customer")]
    private sealed class Customer3
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    [SQLiteTable(Name = "Customer")]
    private sealed class Customer4
    {
        [SQLiteColumn(IsPrimaryKey = true, Name = "Id")]
        public long Identifier { get; set; }

        [SQLiteIndex("NameIndex")]
        public string FirstName { get; set; }

        [SQLiteIndex("NameIndex")]
        public string LastName { get; set; }
    }

    [SQLiteTable(Name = "Customer")]
    private sealed class AllTypes
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

    private sealed class AnnotatedDb(string filePath) : SQLiteDatabase(filePath)
    {
        protected override SQLiteObjectTableBuilder CreateObjectTableBuilder(Type type, SQLiteBuildTableOptions options = null) => new AnnotatedBuilder(this, type, options);

        private sealed class AnnotatedBuilder(SQLiteDatabase database, Type type, SQLiteBuildTableOptions options = null) : SQLiteObjectTableBuilder(database, type, options)
        {
            protected override SQLiteColumnAttribute AddAnnotationAttributes(PropertyInfo property, SQLiteColumnAttribute attribute)
            {
                ArgumentNullException.ThrowIfNull(property);

                // attribute may be null here

                var ignore = property.GetCustomAttribute<NotMappedAttribute>();
                if (ignore != null)
                {
                    attribute ??= CreateColumnAttribute();
                    attribute.Ignore = true;
                }

                var col = property.GetCustomAttribute<ColumnAttribute>();
                if (col != null)
                {
                    attribute ??= CreateColumnAttribute();
                    if (!string.IsNullOrWhiteSpace(col.Name))
                    {
                        attribute.Name = col.Name;
                    }

                    if (col.Order != -1)
                    {
                        attribute.SortOrder = col.Order;
                    }

                    if (!string.IsNullOrWhiteSpace(col.TypeName))
                    {
                        attribute.DataType = col.TypeName;
                    }
                }

                var key = property.GetCustomAttribute<KeyAttribute>();
                if (key != null)
                {
                    attribute ??= CreateColumnAttribute();
                    attribute.IsPrimaryKey = true;
                }

                var req = property.GetCustomAttribute<RequiredAttribute>();
                if (req != null)
                {
                    attribute ??= CreateColumnAttribute();
                    attribute.IsNullable = false;
                }

                var gen = property.GetCustomAttribute<DatabaseGeneratedAttribute>();
                if (gen != null && gen.DatabaseGeneratedOption != DatabaseGeneratedOption.None)
                {
                    switch (gen.DatabaseGeneratedOption)
                    {
                        case DatabaseGeneratedOption.Identity:
                            attribute ??= CreateColumnAttribute();
                            attribute.AutoIncrements = true;
                            break;
                    }
                }
                return attribute;
            }
        }
    }

    // this uses data annotations
    private sealed class AnnotatedCustomer
    {
        public AnnotatedCustomer()
        {
            Id = Guid.NewGuid();
        }

        [Key]
        public Guid Id { get; set; }

        public Guid? NullableGuid { get; set; }

        [Required]
        public Guid? NotNullableGuid { get; set; }

        public Guid Guid { get; set; }

        [Column("OtherName")]
        public string Name { get; set; }

        [NotMapped]
        public bool IsEmptyId => Id == Guid.Empty;
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
