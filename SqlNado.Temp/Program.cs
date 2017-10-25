using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Dynamic;
using System.Linq;
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
                db.DeleteTempTables();
                db.Tables.ToTableString(Console.Out);
                Console.WriteLine(db.EnforceForeignKeys);
                var table = db.GetObjectTable<Customer>();
                var o = new SQLiteSaveOptions { DeleteUnusedColumns = true };

                var c = new Customer();
                c.Name = "Name" + DateTime.Now;
                db.Save(c);
                db.Tables.ToTableString(Console.Out);
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

    public class Customer : ISQLiteObject
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
        [SQLiteColumn(HasDefaultValue = true, IsDefaultValueIntrinsic = true, DefaultValue = "CURRENT_TIMESTAMP")]
        public DateTime CreationDate { get; set; }

        public void OnLoadAction(SQLiteObjectAction action, SQLiteStatement statement, SQLiteLoadOptions options)
        {
        }

        public void OnSaveAction(SQLiteObjectAction action, SQLiteSaveOptions options)
        {
        }
    }
}
