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
* Automatic support for Windows 'winsqlite3.dll' to avoid shipping any binary file.

## Requirements
The only requirement is netstandard 2.0 or .NET Framework 4.6. It's 100% dependency free! Well, of course it requires an SQLite native dlls corresponding to the bitness (x86 vs x64) of the executing app. Note that it's only been validated on the Windows 32 and 64-bit platforms.

## Installation
If you're running on a recent Windows 10 or Windows Server 2016, there is a good chance that there's already a winsqlite3.dll present in \Windows\System32. If this is the case, you won't need to install any native dll, whatever the bitness (x86 vs x64) of your app is!
Note this is true on Azure Web Apps, you don't need to add anything to be able to work with SQLite if you use SQLNado.

Otherwise, you can use the sqlite dll files from https://www.sqlite.org/download.html. We recommend to rename the original sqlite.dll for 32 *and* 64-bit to sqlite.x86.dll and sqlite.x64.dll respectively.
Or you can get them from here already renamed: https://github.com/smourier/SQLNado/tree/master/SqlNado.
Once you have these files, you can copy them aside your running executable (or *bin* directory for a web site).

SQLNado source code expects that and this way your program will be able to run as 32-bit or as 64-bit without having to change the native sqlite.dll. You won't have to build two setups either. 

If you don't like all this and want to keep the original SQLite dll untouched, you can just copy the corresponding standard sqlite.dll aside your running executable also, but make sure you use the proper 32 or 64-bit version.

## Amalgamation
Although SQLNado can be used as a Nuget, we also provide it as a single, 100% independent, C# file located here https://github.com/smourier/SQLNado/tree/master/Amalgamation

The amalgamation contains everything an application needs to embed SQLNado. Combining all the code for SQLNado into one big file makes it easier to deploy — there is just one file to keep track of (yeah, I borrowed that phrase from SQLite's site https://www.sqlite.org/amalgamation.html page because it's cool)

## Nuget
Due to popular request, SQLNado is now also provided as a Nuget. You can download here from https://www.nuget.org/packages/SqlNado/

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
