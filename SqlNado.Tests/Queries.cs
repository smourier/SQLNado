using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlNado.Tests;

[TestClass]
public class Queries
{
    [TestMethod]
    public void TestQueryable()
    {
        using var db = new SQLiteDatabase(":memory:");
        db.SynchronizeSchema<Customer1>();
        int max = 10;
        for (int i = 0; i < max; i++)
        {
            var customer = new Customer1
            {
                Id = i,
                Name = "Name" + i
            };
            db.Save(customer, new SQLiteSaveOptions(db));
        }

        var table = db.GetTable<Customer1>();
        Assert.HasCount(2, table.Columns);
        Assert.AreEqual(nameof(SQLiteColumnType.INTEGER), table.GetColumn("Id").Type);
        Assert.AreEqual(nameof(SQLiteColumnType.TEXT), table.GetColumn("Name").Type);
        Assert.AreEqual(max, db.GetTableRows<Customer1>().Count());

        var list = db.Query<Customer1>().Where(c => c.Name.StartsWith("name", StringComparison.OrdinalIgnoreCase)).Skip(2).Take(2).ToList();
        Assert.HasCount(2, list);
    }

    [TestMethod]
    public void TestParametric()
    {
        using var db = new SQLiteDatabase(":memory:");
        int max = 10;
        for (int i = 0; i < max; i++)
        {
            var customer = new Customer1
            {
                Id = i,
                Name = (i % 2) == 0 ? "Even" + i : "Odd" + i
            };
            db.Save(customer);
        }

        var evens = db.Load<Customer1>("WHERE name LIKE ?", "Even%").ToArray();
        Assert.HasCount(5, evens);

        var odds = db.Load<Customer1>("WHERE name LIKE ?", "Odd%").ToArray();
        Assert.HasCount(5, odds);
    }

    [SQLiteTable(Name = "Customer")]
    private sealed class Customer1
    {
        public int Id { get; set; }
        public string Name { get; set; }

        public override string ToString() => Name;
    }
}
