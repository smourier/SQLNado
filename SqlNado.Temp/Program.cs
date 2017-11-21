using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
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
            using (var db = new SQLiteDatabase("test.db"))
            {
                //db.Tables.ToList().ForEach(t => t.Delete());
                //db.DeleteTempTables();
                db.DeleteTable<UserWithBlob>();
                db.DeleteTable<Product>();
                db.Vacuum();
                db.Tables.ToTableString(Console.Out);

                //var pro = db.CreateObjectInstance<Product>();
                //pro.User = db.CreateObjectInstance<User>();
                //pro.User.Name = "bob" + Environment.TickCount;
                //pro.User.Email = "toto@titi.com";
                //pro.User.Save();
                //pro.Save();
                //TableStringExtensions.ToTableString(db.GetTable<Product>().GetRows(), Console.Out);
                //var prod = db.LoadAll<Product>().First();
                //TableStringExtensions.ToTableString(prod, Console.Out);
                //TableStringExtensions.ToTableString(prod.User, Console.Out);

                //return;
                //db.Logger = new ConsoleLogger(true);

                db.BeginTransaction();
                for (int i = 0; i < 10; i++)
                {
                    var c = db.CreateObjectInstance<UserWithBlob>();
                    c.Email = "bob" + i + "." + Environment.TickCount + "@mail.com";
                    c.Name = "Name" + i + DateTime.Now;
                    c.Options = UserOptions.Super;
                    db.Save(c);
                    //c.Photo = File.ReadAllBytes(@"d:\temp\IMG_0803.JPG");
                    //c.Photo.Save(@"d:\temp\IMG_0803.JPG");

                    //var p = db.CreateObjectInstance<Product>();
                    //p.Id = Guid.NewGuid();
                    //p.User = c;
                    //db.Save(p);
                }
                db.Commit();

                var table = db.GetTable<UserWithBlob>();
                if (table != null)
                {
                    TableStringExtensions.ToTableString(table, Console.Out);
                    //TableStringExtensions.ToTableString(table.GetRows(), Console.Out);
                    var one = db.LoadAll<UserWithBlob>().FirstOrDefault();
                    one.Photo.Load("test.jpg");
                    TableStringExtensions.ToTableString(one, Console.Out);
                }

                var table2 = db.GetTable<Product>();
                if (table2 != null)
                {
                    TableStringExtensions.ToTableString(table2, Console.Out);
                    TableStringExtensions.ToTableString(table2.GetRows(), Console.Out);
                }


                var query = new SQLiteQuery<UserWithBlob>(db);
                string tot = "toto";
                foreach (var user in query.Where(u => u.Name == null))
                {
                    Console.WriteLine(user.Name);
                }
                //db.LoadAll<User>().ToTableString(Console.Out);
            }
        }

        private static void OnPropertyRollback(object sender, DictionaryObjectPropertyRollbackEventArgs e)
        {
            Console.WriteLine("OnPropertyRollback sender:" + sender + " name: " + e.PropertyName + " value: " + e.ExistingProperty?.Value + " invalid: " + e.InvalidValue);
        }

        private static void OnPropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            var ee = (DictionaryObjectPropertyChangedEventArgs)e;
            Console.WriteLine("OnPropertyChanged sender:" + sender + " name: " + e.PropertyName + " old:" + ee.ExistingProperty?.Value + " new:" + ee.NewProperty.Value);
        }

        private static void OnPropertyChanging(object sender, PropertyChangingEventArgs e)
        {
            var ee = (DictionaryObjectPropertyChangingEventArgs)e;
            Console.WriteLine("OnPropertyChanging sender:" + sender + " name: " + e.PropertyName + " old:" + ee.ExistingProperty?.Value + " new:" + ee.NewProperty.Value);
        }

        private static void OnErrorsChanged(object sender, DataErrorsChangedEventArgs e)
        {
            Console.WriteLine("OnErrorsChanged sender:" + sender + " name: " + e.PropertyName);
            var errors = ((INotifyDataErrorInfo)sender).GetErrors(null);
            if (errors == null)
            {
                Console.WriteLine(" OnErrorsChanged no more error.");
                return;
            }

            foreach (var obj in errors)
            {
                Console.WriteLine(" OnErrorsChanged error: " + obj);
            }
        }

    }

    public class SimpleUser
    {
        [SQLiteColumn(IsPrimaryKey = true)]
        public string Email { get; set; }
        public string Name { get; set; }
    }

    public class User : SQLiteBaseObject
    {
        public User(SQLiteDatabase db)
            : base(db)
        {
        }

        [SQLiteColumn(IsPrimaryKey = true)]
        public string Email { get; set; }
        public string Name { get; set; }
        public byte[] Photo { get; set; }

        public IEnumerable<Product> Products => LoadByForeignKey<Product>();

        public override string ToString() => "'" + Email + "'";
    }

    [Flags]
    public enum UserOptions
    {
        None = 0x0,
        Cool = 0x1,
        Super = 0x1000,
    }

    public class UserWithBlob : SQLiteBaseObject
    {
        public UserWithBlob(SQLiteDatabase db)
            : base(db)
        {
            Photo = new SQLiteBlobObject(this, nameof(Photo));
        }

        [SQLiteColumn(IsPrimaryKey = true)]
        public string Email { get; set; }
        public string Name { get; set; }
        public SQLiteBlobObject Photo { get; }
        public UserOptions Options { get; set; }

        //public IEnumerable<Product> Products => LoadByForeignKey<Product>();

        public override string ToString() => "'" + Email + "'";
    }

    public class Product : SQLiteBaseObject
    {
        public Product(SQLiteDatabase db)
            : base(db)
        {
            Id = Guid.NewGuid();
        }

        [SQLiteColumn(IsPrimaryKey = true)]
        public Guid Id { get => DictionaryObjectGetPropertyValue<Guid>(); set => DictionaryObjectSetPropertyValue(value, DictionaryObjectPropertySetOptions.RollbackChangeOnError); }
        public User User { get => DictionaryObjectGetPropertyValue<User>(); set => DictionaryObjectSetPropertyValue(value); }

        public override string ToString() => "'" + Id + "'";

        protected override IEnumerable DictionaryObjectGetErrors(string propertyName)
        {
            if (propertyName == null || propertyName == nameof(Id))
            {
                if (Id == Guid.Empty)
                    return new[] { "Id must be non empty." };
            }

            return null;
        }
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

    public class Customer : ISQLiteObjectEvents
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
