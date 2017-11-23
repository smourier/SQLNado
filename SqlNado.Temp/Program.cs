using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
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
            using (var db = new SQLiteDatabase("test.db"))
            {
                db.Logger = new ConsoleLogger(true);
                db.CollationNeeded += OnCollationNeeded;
                db.DefaultColumnCollation = nameof(StringComparer.OrdinalIgnoreCase);

                //db.DeleteTable<UserWithBlob>();
                //db.DeleteTable<Product>();
                //db.Vacuum();
                //db.SynchronizeSchema<TestQuery>();

                //TestQuery.Ensure(db);
                db.LoadAll<TestQuery>().ToTableString(Console.Out);

                //db.BeginTransaction();
                //for (int i = 0; i < 10; i++)
                //{
                //    var c = db.CreateObjectInstance<UserWithBlob>();
                //    c.Email = "bob" + i + "." + Environment.TickCount + "@mail.com";
                //    c.Name = "Name" + i + DateTime.Now;
                //    c.Options = UserOptions.Super;
                //    db.Save(c);
                //    //c.Photo = File.ReadAllBytes(@"d:\temp\IMG_0803.JPG");
                //    //c.Photo.Save(@"d:\temp\IMG_0803.JPG");

                //    //var p = db.CreateObjectInstance<Product>();
                //    //p.Id = Guid.NewGuid();
                //    //p.User = c;
                //    //db.Save(p);
                //}
                //db.Commit();

                //var table = db.GetTable<UserWithBlob>();
                //if (table != null)
                //{
                //    TableStringExtensions.ToTableString(table, Console.Out);
                //    //TableStringExtensions.ToTableString(table.GetRows(), Console.Out);
                //    var one = db.LoadAll<UserWithBlob>().FirstOrDefault();
                //    one.Photo.Load("test.jpg");
                //    TableStringExtensions.ToTableString(one, Console.Out);
                //}

                //var table2 = db.GetTable<Product>();
                //if (table2 != null)
                //{
                //    TableStringExtensions.ToTableString(table2, Console.Out);
                //    TableStringExtensions.ToTableString(table2.GetRows(), Console.Out);
                //}

                db.SetScalarFunction("toto", 1, true, (c) =>
                {
                    c.SetResult("héllo world");
                });

                while (true)
                {
                    db.LoadRows("SELECT 'toto' = 'tOtO' COLLATE c_1033").ToTableString(Console.Out);
                    GC.Collect();
                }
                //db.Query<TestQuery>().Where(u => u.Department.Contains("h") || u.Department == "accounting").ToTableString(Console.Out);
                //db.Query<TestQuery>().Where(u => u.Department.Substring(1) == "R" || u.Department.Substring(1) == "r").
                //    Select(u => new { D = u.Department }).ToTableString(Console.Out);
                //db.Query<TestQuery>().Where(u => u.Department.Contains("r")).ToTableString(Console.Out);
                //db.Query<TestQuery>().Where(u => u.StartDateUtc > DateTime.UtcNow).ToTableString(Console.Out);
                var sc = StringComparison.CurrentCultureIgnoreCase;
                var eq = EqualityComparer<string>.Default;
                //db.Query<TestQuery>().Where(u => u.Department.IndexOf("h", sc) >= 0).ToTableString(Console.Out);
                string h = "h";
                string r = "r";
                //TableStringExtensions.ToTableString(db.GetTable<TestQuery>(), Console.Out);
                db.GetTable<TestQuery>().Columns.ToTableString(Console.Out);
            }
        }

        private static void OnCollationNeeded(object sender, SQLiteCollationNeededEventArgs e)
        {
            Console.WriteLine("OnCollationNeeded sender:" + sender + " name: " + e.CollationName);
            //e.Database.SetCollationFunction(nameof(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
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
        HasDrivingLicense = 0x1,
        HasTruckDrivingLicense = 0x2,
        IsAdmin = 0x4,
    }

    public class TestQuery : SQLiteBaseObject
    {
        public TestQuery(SQLiteDatabase database) : base(database)
        {
        }

        [SQLiteColumn(IsPrimaryKey = true)]
        public string Name { get; set; }
        public int Age { get; set; }
        public string Department { get; set; }
        public bool IsAbsent { get; set; }
        public UserOptions Options { get; set; }
        public decimal MonthlySalary { get; set; }
        public double OfficeLatitude { get; set; }
        public double OfficeLongitude { get; set; }
        public DateTime StartDateUtc { get; set; }
        public DateTime EndDateUtc { get; set; }
        public Guid UniqueId { get; set; }

        public static void Ensure(SQLiteDatabase db)
        {
            var tq = new TestQuery(db);
            tq.Name = "bill";
            tq.Age = 21;
            tq.Department = "Accounting";
            tq.IsAbsent = false;
            tq.Options = UserOptions.HasDrivingLicense;
            tq.MonthlySalary = 1200;
            tq.OfficeLatitude = 49.310230;
            tq.OfficeLongitude = 24.0923;
            tq.StartDateUtc = new DateTime(1990, 1, 25);
            tq.EndDateUtc = new DateTime(2019, 11, 5);
            tq.UniqueId = new Guid("00000000-0000-0000-0000-000000000001");
            tq.Save();

            tq = new TestQuery(db);
            tq.Name = "samantha";
            tq.Age = 51;
            tq.Department = "HR";
            tq.IsAbsent = true;
            tq.Options = UserOptions.HasDrivingLicense | UserOptions.IsAdmin;
            tq.MonthlySalary = 2120;
            tq.OfficeLatitude = 48.310230;
            tq.OfficeLongitude = 23.0923;
            tq.StartDateUtc = new DateTime(2000, 4, 13);
            tq.UniqueId = new Guid("00000000-0000-0000-1000-000000000002");
            tq.Save();

            tq = new TestQuery(db);
            tq.Name = "joe";
            tq.Age = 32;
            tq.Department = "HR";
            tq.IsAbsent = true;
            tq.Options = UserOptions.HasTruckDrivingLicense | UserOptions.HasDrivingLicense;
            tq.MonthlySalary = 1532;
            tq.OfficeLatitude = 47.310230;
            tq.OfficeLongitude = 22.0923;
            tq.StartDateUtc = new DateTime(2014, 10, 11);
            tq.UniqueId = new Guid("00000000-0000-1000-1000-000000000003");
            tq.Save();

            tq = new TestQuery(db);
            tq.Name = "will";
            tq.Age = 34;
            tq.Department = "HR";
            tq.IsAbsent = true;
            tq.Options = UserOptions.None;
            tq.MonthlySalary = 1370;
            tq.OfficeLatitude = 46.110230;
            tq.OfficeLongitude = 25.1923;
            tq.StartDateUtc = new DateTime(2005, 1, 4);
            tq.EndDateUtc = new DateTime(2011, 4, 2);
            tq.UniqueId = new Guid("00000000-5000-1000-1000-000000000004");
            tq.Save();

            tq = new TestQuery(db);
            tq.Name = "leslie";
            tq.Age = 44;
            tq.Department = "Accounting";
            tq.IsAbsent = true;
            tq.Options = UserOptions.None;
            tq.MonthlySalary = 2098;
            tq.OfficeLatitude = 47.110230;
            tq.OfficeLongitude = 26.1923;
            tq.StartDateUtc = new DateTime(2001, 7, 20);
            tq.UniqueId = new Guid("A0000000-5000-1000-1000-000000000005");
            tq.Save();

            tq = new TestQuery(db);
            tq.Name = "bob";
            tq.Age = 36;
            tq.Department = "Accounting";
            tq.IsAbsent = false;
            tq.Options = UserOptions.HasDrivingLicense;
            tq.MonthlySalary = 2138;
            tq.OfficeLatitude = 48.109630;
            tq.OfficeLongitude = 25.1923;
            tq.StartDateUtc = new DateTime(2001, 5, 10);
            tq.UniqueId = new Guid("A0000000-5000-1000-1000-000000000006");
            tq.Save();

            tq = new TestQuery(db);
            tq.Name = "tom";
            tq.MonthlySalary = decimal.MaxValue;
            tq.Save();
        }
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
