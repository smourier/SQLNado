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
                db.DeleteTable<Customer>();
                //db.Tables.ToTableString(Console.Out);
                //Console.WriteLine(db.EnforceForeignKeys);
                //var table = db.GetObjectTable<Customer>();
                //var o = new SQLiteSaveOptions { DeleteUnusedColumns = true };

                var c = new Customer();
                c.Name = "Name" + DateTime.Now;
                db.Save(c);
                var ot = db.GetObjectTable<Customer>();
                TableStringExtensions.ToTableString(ot, Console.Out);
                TableStringExtensions.ToTableString(c, Console.Out);

                //db.DeleteTable("t");
                //db.ExecuteNonQuery("CREATE TABLE t(x INTEGER PRIMARY KEY, y, z) WITHOUT ROWID;");
                //db.ExecuteAsRows("PRAGMA index_xinfo(sqlite_autoindex_t_1)").ToTableString(Console.Out);
                ////db.ExecuteAsRows("SELECT rowid,* FROM invoices limit 10").ToTableString(Console.Out);
                var table = db.GetTable<Customer>();
                //Console.WriteLine(table.Sql);
                //table.Columns.ToTableString(Console.Out);
                //table.Indices.ToTableString(Console.Out);
                TableStringExtensions.ToTableString(table, Console.Out);
                TableStringExtensions.ToTableString(table.GetRows(), Console.Out);
            }
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

    [SQLiteTable(Name = "playlist_track")]
    public class PlayListTrack
    {
        public int PlaylistId { get; set; }
        public int TrackId { get; set; }
    }

    [SQLiteTable(Name = "Customers")]
    public class CustomerWithRowId
    {
        public long RowId { get; set; }
    }

    public class Customer : ISQLiteObject
    {
        public Customer()
        {
            //Age = 20;
            Name = "Customer" + Environment.TickCount;
            Id = Guid.NewGuid();
            CreationDate = DateTime.Now;
        }

        [SQLiteColumn(IsPrimaryKey = true)]
        public Guid Id { get; }
        public string Name { get; set; }
        [SQLiteColumn(AutomaticType = SQLiteAutomaticColumnType.Random)]
        public int Age { get; set; }
        [SQLiteColumn(HasDefaultValue = true, IsDefaultValueIntrinsic = true, DefaultValue = "CURRENT_TIMESTAMP")]
        public DateTime CreationDate { get; set; }

        public object[] PrimaryKey => new object[] { Id };

        public bool OnLoadAction(SQLiteObjectAction action, SQLiteStatement statement, SQLiteLoadOptions options)
        {
            return true;
        }

        public bool OnSaveAction(SQLiteObjectAction action, SQLiteSaveOptions options)
        {
            return true;
        }
    }
}
