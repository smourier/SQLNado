# SQLNado
SQLNado (SQLite Not ADO) is a .NET lightweight bloat free object persistence framework based on SQLite.

SQLNado supports all of SQLite features when using SQL commands, and also supports most of SQLite features through .NET:

* Automatic class-to-table mapping (Save, Delete, Load, LoadAll, LoadByPrimaryKey, LoadByForeignKey, etc.)
* Automatic synchronization of schema (tables, columns) between classes and existing table
* Designed for thread-safe operations
* Where and OrderBy LINQ/IQueryable .NET expressions are supported (work is still in progress in this area), also with collation support
* SQLite database schema (tables, columns, etc.) exposed to .NET
* SQLite custom functions can be written in .NET
* SQLite incremental BLOB I/O is exposed as a .NET Stream to avoid high memory consumption
* SQLite  collation support, including the possibility to add custom collations using .NET code
* SQLite Full Text Search engine (FTS3/4) support, including the possibility to add custom FTS3 tokenizers using .NET code
* Automatic support for Windows 'winsqlite3.dll' to avoid shipping any binary file.

## Cross-Platform
Since version 2, the whole of SQLNado is now cross-platform enabled. It currently supports Linux, Windows and Android, but more will follow (iOS, MacOS, etc.). Obviously, you need a platform with C# support.

*Note: the Nuget is still currently on the old version as the version 2 is still being tested*.

## Requirements
On **Windows**, the only requirement is netstandard 2.0 or .NET Framework 4.6, so it works fine with .NET Core to .NET 9+ too. It's 100% dependency free! Well, of course it requires an SQLite native dlls corresponding to the bitness (x86 vs x64) of the executing app.

On **Linux**, the only requirement, beside .NET Core, is to have libsqlite3.so (or similar) available to the project you're working on.

On **Android** (Xamarin/MAUI), the only requirement is to have some version of the sqlite shared library available to the project you're working on.

You can use the library proposed by sqlite.org itself, ie:

    * download library from https://www.sqlite.org/download.html ("Precompiled Binaries for Android")
    * unzip it and copy "sqlite-android-[some version like 3410000]\jni" directory into your "Platform\Android" directory or somewhere suitable
    * set "Build Action" of all .so to "AndroidNativeLibrary"

But you can also used other libraries (like the one from built by https://github.com/ericsink/SQLitePCL.raw).

## Installation on Windows
If you're running on a recent Windows 10 or Windows Server 2016+, there is a good chance that there's already a `winsqlite3.dll` file present in `\Windows\System32`. If this is the case, you won't need to install any native dll, whatever the bitness (x86 vs x64) of your app is!

Note this is also true on Azure Web Apps, you don't need to add anything to be able to work with SQLite if you use SQLNado.

Otherwise, you can use the sqlite dll files from https://www.sqlite.org/download.html. We usually recommend to rename the original sqlite.dll for 32 *and* 64-bit to `sqlite.x86.dll` and `sqlite.x64.dll` respectively so you can distribute both in the same directory.

Or you can get them from here already renamed: https://github.com/smourier/SQLNado/tree/master/SqlNado.

Once you have these files, you can copy them aside your running executable (or *bin* directory for a web site).
SQLNado source code expects that and this way your program will be able to run as 32-bit or as 64-bit without having to change the native sqlite .dll. You won't have to build two setups either. 

If you don't like all this and want to keep the original SQLite dll untouched, or even bring your own SQLite dll, you can just copy the corresponding .dll aside your running executable also, but make sure you use the proper 32 or 64-bit version.

## Installation on Linux
We recommend to use SQLNado amalgamation (one unique .cs file). In this mode you just need to to make sure *libsqlite3-0* or *libsqlite3-dev* is installed and accessible to the .NET project you're working on, there's nothing specific to do beside include `sqlnado.cs`.

## Installation on Android
We recommend to use SQLNado amalgamation (one unique .cs file). In this mode you just need to to make sure the SQLite Android shared library is packaged with the .NET project you're working on, there's nothing specific to do beside include `sqlnado.cs`.
See here for more on this subject: https://learn.microsoft.com/en-us/xamarin/android/platform/native-libraries

## Amalgamation
Although SQLNado can be used as a Nuget, we also provide it as a single, 100% independent, C# file located here https://github.com/smourier/SQLNado/tree/master/Amalgamation

The amalgamation contains everything an application needs to embed SQLNado. Combining all the code for SQLNado into one big file makes it easier to deploy — there is just one file to keep track of (yeah, I borrowed that phrase from SQLite's site https://www.sqlite.org/amalgamation.html page because it's cool)

## Nuget
Due to popular request, SQLNado is now also provided as a Nuget. You can download here from https://www.nuget.org/packages/SqlNado/
*Note: the Nuget is still currently on the old version as the version 2 is being tested*

## Get Started
Here is a simple Console App that should get you started:

```csharp
// this will create the my.db file if it does not exists
using (var db = new SQLiteDatabase("my.db"))
{
    var customer = new Customer();
    customer.Email = "killroy@example.com";
    customer.Name = "Killroy";

    // updates or Insert (choice is made if there is already a primary key on the object).
    // by default, the Save operation synchronize the table schema.
    // if this is run for the first time, it will create the table using Customer type definition (properties).
    db.Save(customer);

    // dumps the customer list to the console
    db.LoadAll<Customer>().ToTableString(Console.Out);

    // dumps the sql query result to the console (should be the same as previous)
    db.LoadRows("SELECT * FROM Customer").ToTableString(Console.Out);

    // dumps the Customer table schema to the console
    TableStringExtensions.ToTableString(db.GetTable<Customer>(), Console.Out);

    // dumps the Customer table columns definitions to the console
    db.GetTable<Customer>().Columns.ToTableString(Console.Out);
}

public class Customer
{
    [SQLiteColumn(IsPrimaryKey = true)]
    public string Email { get; set; }
    public string Name { get; set; }
}
```
When you run it, you should see this on the console.
![Console Output](/Doc/Images/TableString1.png?raw=true)

These nice table outputs are created automatically by the [TableString](/SqlNado/Utilities/TableString.cs) utility that's part of SQLNado (but the file can be copied in any other C# project as it's self-sufficient).

`TableString` computes tables from any `IEnumerable` instance. It works also for any object, like the Customer table schema example, but for object that are not IEnumerable there's not extension method, you have to use TableString or TableStringExtensions). What's cool is it computes columns widths so can fit within the console bounds. When working with tables from a database, it's *very* useful.

## FTS custom tokenizer support
SQLNado offers the possibility to use a custom FTS3 tokenizer to SQLite FTS engine. This allows you to code a stop word tokenizer for example. Here is a sample code:

```csharp
using (var db = new SQLiteDatabase(":memory:"))
{
    // we must enable custom tokenizers
    db.Configure(SQLiteDatabaseConfiguration.SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER, 1);
    var tok = new StopWordTokenizer(db);
    db.SetTokenizer(tok);

    // insert data as in SQLite exemple
    db.Save(new Mail { docid = 1, Subject = "software feedback", Body = "found it too slow" });
    db.Save(new Mail { docid = 2, Subject = "software feedback", Body = "no feedback" });
    db.Save(new Mail { docid = 3, Subject = "slow lunch order", Body = "was a software problem" });

    // check result
    db.LoadAll<Mail>().ToTableString(Console.Out);

    // this shall report the same as in SQLite example
    db.Load<Mail>("WHERE Subject MATCH 'software'").ToTableString(Console.Out);
    db.Load<Mail>("WHERE body MATCH 'feedback'").ToTableString(Console.Out);
    db.Load<Mail>("WHERE mail MATCH 'software'").ToTableString(Console.Out);
    db.Load<Mail>("WHERE mail MATCH 'slow'").ToTableString(Console.Out);

    // this should display nothing as 'no' is a stop word
    db.Load<Mail>("WHERE mail MATCH 'no'").ToTableString(Console.Out);
}

[SQLiteTable(Module = "fts3", ModuleArguments = nameof(Subject) + ", " + nameof(Body) + ", tokenize=" + StopWordTokenizer.TokenizerName)]
public class Mail
{
    public long docid { get; set; }
    public string Subject { get; set; }
    public string Body { get; set; }

    public override string ToString() => Subject + ":" + Body;
}

public class StopWordTokenizer : SQLiteTokenizer
{
    public const string TokenizerName = "unicode_stopwords";

    private static readonly HashSet<string> _words;
    private readonly SQLiteTokenizer _unicode;
    private int _disposed;

    static StopWordTokenizer()
    {
        // define some stop words here
        _words = new HashSet<string> {
            "a",
            "it",
            "no",
            "too",
            "was",
            };
    }

    public StopWordTokenizer(SQLiteDatabase database, params string[] arguments)
        : base(database, TokenizerName)
    {
        // we reuse SQLite's unicode61 tokenize
        _unicode = database.GetUnicodeTokenizer(arguments);
    }

    public override IEnumerable<SQLiteToken> Tokenize(string input)
    {
        foreach (var token in _unicode.Tokenize(input))
        {
            if (!_words.Contains(token.Text))
                yield return token;
        }
    }

    // we need this because SQLite's unicode61 tokenizer aggregates unmanaged resources
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
}
```
When you run it, you should see this on the console.
![Console Output](/Doc/Images/Fts.png?raw=true)
