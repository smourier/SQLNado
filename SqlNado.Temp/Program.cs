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
                //db.Logger = new ConsoleLogger(true);
                db.DeleteTable<User>();
                db.DeleteTable<Product>();
                db.DeleteTempTables();

                for (int i = 0; i < 10; i++)
                {
                    var c = new User();
                    c.Email = "bob" + i + "." + Environment.TickCount + "@mail.com";
                    c.Name = "Name" + i + DateTime.Now;
                    db.Save(c);

                    var p = new Product();
                    p.Id = Guid.NewGuid();
                    p.User = c;
                    db.Save(p);
                }


                var table = db.GetTable<User>();
                TableStringExtensions.ToTableString(table, Console.Out);
                TableStringExtensions.ToTableString(table.GetRows(), Console.Out);

                var table2 = db.GetTable<Product>();
                TableStringExtensions.ToTableString(table2, Console.Out);
                TableStringExtensions.ToTableString(table2.GetRows(), Console.Out);

                //db.LoadAll<User>().ToTableString(Console.Out);
            }
        }
    }

    public class User
    {
        [SQLiteColumn(IsPrimaryKey = true)]
        public string Email { get; set; }
        public string Name { get; set; }
    }

    public class Product
    {
        [SQLiteColumn(IsPrimaryKey = true)]
        public Guid Id { get; set; }
        public User User { get; set; }
    }

    public class CustomerWithRowId
    {
        [SQLiteColumn(IsPrimaryKey = true, AutoIncrements = true)]
        public long MyRowId { get; set; }
        public string Name { get; set; }
        [SQLiteColumn(HasDefaultValue = true, IsDefaultValueIntrinsic = true, DefaultValue = "CURRENT_TIMESTAMP")]
        public DateTime CreationDate { get; set; }
    }

    public class CustomerWithImplicitRowId
    {
        [SQLiteColumn(IsPrimaryKey = true, AutoIncrements = true)]
        public byte MyRowId { get; set; }
        public string Name { get; set; }
        [SQLiteColumn(HasDefaultValue = true, IsDefaultValueIntrinsic = true, DefaultValue = "CURRENT_TIMESTAMP")]
        public DateTime? CreationDate { get; set; }
    }

    [SQLiteTable(WithoutRowId = true)]
    public class MyLog
    {
        public MyLog()
        {
            CreationDate = DateTime.Now;
            Id = Guid.NewGuid();
            Text = "mylog" + Environment.TickCount;
        }

        [SQLiteColumn(IsPrimaryKey = true)]
        public Guid Id { get; }
        public DateTime CreationDate { get; }
        public string Text { get; set; }
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
        public Guid Id { get; set; }
        public string Name { get; set; }
        [SQLiteColumn(AutomaticType = SQLiteAutomaticColumnType.Random)]
        public int Age { get; set; }
        //[SQLiteColumn(HasDefaultValue = true, IsDefaultValueIntrinsic = true, DefaultValue = "CURRENT_TIMESTAMP")]
        public DateTime CreationDate { get; set; }

        [SQLiteColumn(Ignore = true)]
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
