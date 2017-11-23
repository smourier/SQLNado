# SQLNado
SQLNado (SQLite Not ADO) is a lightweight object persistence framework based on SQLite.

SQLNado supports all of SQLite features when using SQL commands, and also supports most of SQLite features through .NET:

* automatic class-to-table mapping (Save, Delete, etc.)
* SQLite schema (Tables, Columns, etc.) exposed to .NET
* SQLite custom functions can be written in .NET
* SQLite incremental BLOB I/O is exposed as a .NET Stream to avoid high memory consumption
* Where and OrderBy LINQ/IQueryable .NET expressions are supported (work is still in progress in this area), also with collation support
* full collation support
