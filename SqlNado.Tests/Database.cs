using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlNado.Tests
{
    [TestClass]
    public class Database
    {
        [TestMethod]
        public void CreateDatase()
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
    }
}
