using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            using (var db = new SQLiteDatabase("chinook.db"))
            {
                //var value = db.ExecuteAsRows("SELECT * FROM sqlite_master WHERE type='table'");
                //value.ToTableString(Console.Out);
                var table = db.GetObjectTable<Customer>();
                TableStringExtensions.ToTableString(true, Console.Out);
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

    public class Customer
    {
        [SQLiteColumn(IsPrimaryKey = true)]
        public Guid Id { get; }
        public string Name { get; }
        public int Age { get; set; }
    }
}
