using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlNado.Tests
{
    [TestClass]
    public class GeneratedValues
    {
        [TestMethod]
        public void AutomaticColumn_AutoIncrement()
        {
            using (var db = new SQLiteDatabase(":memory:"))
            {
                db.Save(new AutoIncrement());
                db.Save(new AutoIncrement());
                db.Save(new AutoIncrement());

                var customers = db.LoadAll<AutoIncrement>();
                CollectionAssert.AreEquivalent(new[] { 1, 2, 3 }, customers.Select(c => c.Id).ToList());
            }
        }

        internal class AutoIncrement
        {
            [SQLiteColumn(IsPrimaryKey = true, AutoIncrements = true)]
            public int Id { get; set; }
        }
    }
}
