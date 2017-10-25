using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Dynamic;
using System.Text;
using SqlNado.Utilities;

namespace SqlNado.Temp
{
    class Program
    {
        static void Main(string[] args)
        {
            if (Debugger.IsAttached)
            {
                SafeMain(args);
                return;
            }

            try
            {
                SafeMain(args);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        static void SafeMain(string[] args)
        {
            //int? input = null;
            Rectangle rect = new Rectangle(10, 20, 30, 40);
            var input = "10; 20; 30; 40";
            Console.WriteLine(Conversions.TryChangeType(input, typeof(Rectangle), out object value));
            //Console.WriteLine(Conversions.TryChangeType(value, typeof(int), out value));
            TableStringExtensions.ToTableString(value, Console.Out);
            return;
            using (var db = new SQLiteDatabase("chinook.db"))
            {
                //var value = db.ExecuteAsRows("SELECT * FROM sqlite_master WHERE type='table'");
                //value.ToTableString(Console.Out);
                var table = db.GetObjectTable<Customer>();
                var pk = table.GetValues(new Customer());

                db.Tables.ToTableString(Console.Out);
                db.Indices.ToTableString(Console.Out);
                //var rows = db.ExecuteAsRows("SELECT * FROM customers");
                TableStringExtensions.ToTableString(table, Console.Out);
            }

            //dynamic o = new ExpandoObject();
            //o.Name = "toto";
            //o.Whatever = 12;

            //dynamic o3 = new ExpandoObject();
            //o3.stuff = "xxtoto";
            //o3.Name = 1234;

            //var s = new SQLiteRow(0, new[] { "zz" }, new object[] { 123 });
            //object o2 = s;

            //TableStringExtensions.ToTableString(new object[] { o, o3 }, Console.Out);
        }
    }

    [SQLiteTable(Name = "sqlite_master")]
    public class Table
    {
        public string Type { get; set; }
        public string Name { get; set; }
        [SQLiteColumn(Name = "tbl_name")]
        public string TableName { get; set; }
        public int RootPage { get; set; }
        public string Sql { get; set; }
    }

    public class Customer
    {
        public Customer()
        {
            Id = Guid.NewGuid();
            Age = 20;
            Name = "Customer" + Environment.TickCount;
        }

        [SQLiteColumn(IsPrimaryKey = true)]
        public Guid Id { get; }
        public string Name { get; set; }
        public int Age { get; set; }
    }
}
