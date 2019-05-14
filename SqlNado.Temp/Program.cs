using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
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
            using (var db = new SQLiteDatabase(":memory:"))
            {
                for (int i = 0; i < 10; i++)
                {
                    var c = new Customer();
                    c.Name = "name" + i;
                    switch (i)
                    {
                        case 0:
                            c.NullableInt = 1;
                            break;

                        case 2:
                            c.NullableInt = 3;
                            break;

                        case 3:
                            c.NullableInt = 12;
                            break;
                    }
                    db.Save(c);
                }

                var op = db.CreateLoadOptions();
                //op.Offset = 8;
                //op.Limit = 5;
                db.Load<Customer>("SELECT * FROM Customer ORDER BY NullableInt", op).ToTableString(Console.Out);

                foreach (var customer in db.LoadAll<Customer>())
                {
                    Console.WriteLine(customer.Name + " ni:" + customer.NullableInt + " (" + (customer.NullableInt.HasValue ? customer.NullableInt.Value.GetType().FullName : "<null>") + ")");
                }
            }
        }

        static void SafeMain1(string[] args)
        {
            string name = "test.db";
            if (File.Exists(name))
            {
                File.Delete(name);
            }

            using (var db = new SQLiteDatabase(name))
            {
                using (var tok = db.GetTokenizer("unicode61", "remove_diacritics=0", "tokenchars=.=", "separators=X"))
                {
                    Console.WriteLine(string.Join(Environment.NewLine, tok.Tokenize("hello friends")));
                    //GC.Collect(1000, GCCollectionMode.Forced, true);
                }

                var sp = new StopWordTokenizer(db);
                Console.WriteLine(db.Configure(SQLiteDatabaseConfiguration.SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER, true, 1));
                db.SetTokenizer(sp);

                db.ExecuteNonQuery("CREATE VIRTUAL TABLE tok1 USING fts3tokenize('" + sp.Name + "');");

                for (int i = 0; i < 10; i++)
                {
                    var tokens = db.LoadRows(@"SELECT token, start, end, position FROM tok1 WHERE input=?;",
                        "This is a test sentence.");
                    Console.Write(tokens.ToArray().Length);
                    //GC.Collect(1000, GCCollectionMode.Forced, true);
                }
                //Console.WriteLine(string.Join(Environment.NewLine, tokens.Select(t => t["token"])));
            }
        }

        static void SafeMain3(string[] args)
        {
            using (var dic = new PersistentDictionary<string, object>())
            {
                dic.Database.CacheFlush();
                return;
                int max = 10;
                for (int i = 0; i < max; i++)
                {
                    dic[i.ToString()] = i;
                }
                Console.WriteLine(dic.Count);
                Console.WriteLine(dic.Keys.Count);
                Console.WriteLine(dic.Values.Count);

                foreach (var k in dic.Values)
                {
                    Console.WriteLine(k);
                }

                foreach (var kv in dic)
                {
                    Console.WriteLine(kv.Key + "=" + kv.Value + " (" + (kv.Value != null ? kv.Value.GetType().Name : "<null>") + ")");
                }

                var rnd = new Random(Environment.TickCount);

                for (int i = 0; i < max; i++)
                {
                    dic[i.ToString()] = rnd.Next(2) != 0;
                }

                foreach (var kv in dic)
                {
                    Console.WriteLine(kv.Key + "=" + kv.Value + " (" + (kv.Value != null ? kv.Value.GetType().Name : "<null>") + ")");
                }
            }

            using (var dic = new PersistentDictionary<string, string>())
            {
                int max = 10;
                for (byte i = 0; i < max; i++)
                {
                    dic[Guid.NewGuid().ToString()] = i.ToString();
                }

                Console.WriteLine(dic.Count);
                Console.WriteLine(dic.Keys.Count);
                Console.WriteLine(dic.Values.Count);

                foreach (var k in dic.Values)
                {
                    Console.WriteLine(k);
                }

                foreach (var kv in dic)
                {
                    Console.WriteLine(kv.Key + "=" + kv.Value + " (" + (kv.Value != null ? kv.Value.GetType().Name : "<null>") + ")");
                }

                var rnd = new Random(Environment.TickCount);

                for (byte i = 0; i < max; i++)
                {
                    dic[Guid.NewGuid().ToString()] = (rnd.Next(2) != 0).ToString();
                }

                foreach (var kv in dic)
                {
                    Console.WriteLine(kv.Key + "=" + kv.Value + " (" + (kv.Value != null ? kv.Value.GetType().Name : "<null>") + ")");
                }
            }
        }

        static void SafeMain2(string[] args)
        {
            //DocProgram.Starter();

            //return;
            if (File.Exists("test.db"))
            {
                File.Delete("test.db");
            }
            using (var db = new SQLiteDatabase(""))
            {
                //db.Logger = new ConsoleLogger(true);
                db.EnableStatementsCache = true;
                db.CollationNeeded += OnCollationNeeded;
                db.DefaultColumnCollation = nameof(StringComparer.OrdinalIgnoreCase);

                var user = new User(db);
                user.Name = "bob";
                user.Email = "bob@example.com";
                db.Save(user);

                //for (int i = 0; i < 10; i++)
                //{
                //    ThreadPool.QueueUserWorkItem((state) =>
                //    {
                //        int ii = (int)state;
                //        db.SynchronizeSchema<SimpleUser>();

                //        TableStringExtensions.ToTableString(db.GetTable<SimpleUser>(), Console.Out);

                //        var su = new SimpleUser();
                //        su.Name = "toto";
                //        su.Email = "a.b@x.com";
                //        db.Save(su);

                //        db.GetTableRows<SimpleUser>().ToTableString(Console.Out);
                //        db.LoadAll<SimpleUser>().ToTableString(Console.Out);
                //    }, i);
                //}

                Console.ReadLine();
                db.GetStatementsCacheEntries().ToTableString(Console.Out);
                return;

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

                //db.Query<TestQuery>().Where(u => u.Department.Contains("h") || u.Department == "accounting").ToTableString(Console.Out);
                //db.Query<TestQuery>().Where(u => u.Department.Substring(1) == "R" || u.Department.Substring(1) == "r").
                //    Select(u => new { D = u.Department }).ToTableString(Console.Out);
                //db.Query<TestQuery>().Where(u => u.Department.Contains("r")).ToTableString(Console.Out);
                //db.Query<TestQuery>().Where(u => u.StartDateUtc > DateTime.UtcNow).ToTableString(Console.Out);
                var sc = StringComparison.CurrentCultureIgnoreCase;
                var eq = EqualityComparer<string>.Default;
                db.Query<TestQuery>().Where(u => u.Department.Contains("h", sc)).ToTableString(Console.Out);
                db.Query<TestQuery>().Where(u => u.Department.Contains("h", sc)).OrderBy(u => u.Name).ThenByDescending(u => u.MonthlySalary).ToTableString(Console.Out);
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
        public const string GuidEmpt1 = "00000000-0000-0000-0000-000000000001";
        public const string GuidEmpty = "00000000-0000-0000-0000-000000000000";

        public SimpleUser()
        {
            MyGuid = new Guid(GuidEmpt1);
        }

        [SQLiteColumn(IsPrimaryKey = true)]
        public string Email { get; set; }
        public string Name { get; set; }
        [SQLiteColumn(IsNullable = true, DefaultValue = GuidEmpt1, HasDefaultValue = true)]
        public Guid MyGuid { get; set; }

        [SQLiteIndex("myindex")]
        public string Id { get; set; }

        [SQLiteIndex("myindex")]
        public string Id2 { get; set; }

        [SQLiteIndex("toto", IsUnique = true)]
        public string Id3 { get; set; }
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

    public class GeoIPLocation
    {
        [SQLiteColumn(IsPrimaryKey = true)]
        public long GeoNameId { get; set; }
        public string Locale { get; set; }
        public string ContinentCode { get; set; }
        public string ContinentName { get; set; }
        public string CountryIsoCode { get; set; }
        public string CountryName { get; set; }
        public string Subdivision1IsoCode { get; set; }
        public string Subdivision1IsoName { get; set; }
        public string Subdivision2IsoCode { get; set; }
        public string Subdivision2IsoName { get; set; }
        public string CityName { get; set; }
        public string MetroCode { get; set; }
        public string TimeZone { get; set; }
        public int AccuracyRadius { get; set; }
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
        public int? NullableInt { get; set; }

        //[SQLiteColumn(Ignore = true)]
        //public object[] PrimaryKey => new object[] { Id };

        public bool OnLoadAction(SQLiteObjectAction action, SQLiteStatement statement, SQLiteLoadOptions options)
        {
            return true;
        }

        public bool OnSaveAction(SQLiteObjectAction action, SQLiteSaveOptions options)
        {
            return true;
        }
    }

    public class StopWordTokenizer : SQLiteTokenizer
    {
        private readonly SQLiteTokenizer _unicode;
        private int _disposed;
        private readonly static HashSet<string> _words;

        static StopWordTokenizer()
        {
            _words = new HashSet<string>();
            using (var sr = new StringReader(_stopWords))
            {
                do
                {
                    var word = sr.ReadLine();
                    if (word == null)
                        break;

                    _words.Add(word);
                }
                while (true);
            }
        }

        public StopWordTokenizer(SQLiteDatabase database, params string[] arguments)
            : base(database, "unicode_stopwords")
        {
            _unicode = database.GetUnicodeTokenizer(arguments);
        }

        protected override void Dispose(bool disposing)
        {
            var disposed = Interlocked.Exchange(ref _disposed, 1);
            if (disposed != 0)
                return;

            if (disposing)
            {
                _unicode.Dispose();
            }

            base.Dispose(disposing);
        }

        public override IEnumerable<SQLiteToken> Tokenize(string input)
        {
            foreach (var token in _unicode.Tokenize(input))
            {
                // test native mangling stuff...
                GC.Collect(1000, GCCollectionMode.Forced, true);
                if (!_words.Contains(token.Text))
                {
                    yield return token;
                }
            }
        }

        // from https://raw.githubusercontent.com/mongodb/mongo/master/src/mongo/db/fts/stop_words_english.txt
        private const string _stopWords = @"a
about
above
after
again
against
all
am
an
and
any
are
aren't
as
at
be
because
been
before
being
below
between
both
but
by
can't
cannot
could
couldn't
did
didn't
do
does
doesn't
doing
don't
down
during
each
few
for
from
further
had
hadn't
has
hasn't
have
haven't
having
he
he'd
he'll
he's
her
here
here's
hers
herself
him
himself
his
how
how's
i
i'd
i'll
i'm
i've
if
in
into
is
isn't
it
it's
its
itself
let's
me
more
most
mustn't
my
myself
no
nor
not
of
off
on
once
only
or
other
ought
our
ours
ourselves
out
over
own
same
shan't
she
she'd
she'll
she's
should
shouldn't
so
some
such
than
that
that's
the
their
theirs
them
themselves
then
there
there's
these
they
they'd
they'll
they're
they've
this
those
through
to
too
under
until
up
very
was
wasn't
we
we'd
we'll
we're
we've
were
weren't
what
what's
when
when's
where
where's
which
while
who
who's
whom
why
why's
with
won't
would
wouldn't
you
you'd
you'll
you're
you've
your
yours
yourself
yourselves";
    }
}
