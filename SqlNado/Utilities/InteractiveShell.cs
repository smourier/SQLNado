using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;

namespace SqlNado.Utilities
{
    public class InteractiveShell : InteractiveShell<SQLiteDatabase>
    {
    }

    public class InteractiveShell<T> where T : SQLiteDatabase
    {
        public ISQLiteLogger? Logger { get; set; }

        protected virtual bool HandleLine(T database, string line) => false;
        protected virtual T CreateDatabase(string filePath, SQLiteOpenOptions options) => (T)Activator.CreateInstance(typeof(T), new object[] { filePath, options });

        protected virtual void Write(TraceLevel level, string message)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            switch (level)
            {
                case TraceLevel.Error:
                    Console.ForegroundColor = ConsoleColor.Red;
                    break;

                case TraceLevel.Warning:
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    break;

                case TraceLevel.Verbose:
                    Console.ForegroundColor = ConsoleColor.DarkYellow;
                    break;

                case TraceLevel.Off:
                    return;
            }

            try
            {
                Console.WriteLine(message);
            }
            finally
            {
                Console.ResetColor();
            }
        }

        protected virtual void LineHandling(T database)
        {
        }

        protected virtual void LineHandled(T database)
        {
        }

        protected virtual void WriteDatabaseHelp(T datatabase)
        {
            Console.WriteLine();
            Console.WriteLine("Database help");
            Console.WriteLine(" check                Checks database integrity.");
            Console.WriteLine(" clear                Clears the console.");
            Console.WriteLine(" quit                 Exits this shell.");
            Console.WriteLine(" rows <name>          Outputs table rows. Name can contain * wildcard.");
            Console.WriteLine(" stats                Outputs database statistics.");
            Console.WriteLine(" tables               Outputs the list of tables in the database.");
            Console.WriteLine(" table <name>         Outputs table information. Name can contain * wildcard.");
            Console.WriteLine(" this                 Outputs database information.");
            Console.WriteLine(" vacuum               Shrinks the database.");
            Console.WriteLine(" <sql>                Any SQL request.");
            Console.WriteLine();
        }

        protected virtual void WriteHelp(T datatabase) => WriteDatabaseHelp(datatabase);

        public void Run(string filePath) => Run(filePath, SQLiteOpenOptions.SQLITE_OPEN_READWRITE | SQLiteOpenOptions.SQLITE_OPEN_CREATE);
        public virtual void Run(string filePath, SQLiteOpenOptions options)
        {
            if (filePath == null)
                throw new ArgumentNullException(nameof(filePath));

            using (var db = CreateDatabase(filePath, options))
            {
                db.Logger = Logger;
                do
                {
                    LineHandling(db);
                    var line = Console.ReadLine();
                    if (line == null)
                        break;

                    if (line.EqualsIgnoreCase("bye") || line.EqualsIgnoreCase("quit") || line.EqualsIgnoreCase("exit") ||
                        line.EqualsIgnoreCase("b") || line.EqualsIgnoreCase("q") || line.EqualsIgnoreCase("e"))
                        break;

                    if (HandleLine(db, line))
                        continue;

                    if (line.EqualsIgnoreCase("help"))
                    {
                        WriteHelp(db);
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("clear") || line.EqualsIgnoreCase("cls"))
                    {
                        Console.Clear();
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("this"))
                    {
                        TableStringExtensions.ToTableString(db, Console.Out);
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("check"))
                    {
                        Console.WriteLine(db.CheckIntegrity() ? "ok" : "not ok");
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("vacuum"))
                    {
                        db.Vacuum();
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("tables"))
                    {
                        db.Tables.Select(t => new { t.Name, t.RootPage, t.Sql }).ToTableString(Console.Out);
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("indices"))
                    {
                        db.Indices.ToTableString(Console.Out);
                        LineHandled(db);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("stats"))
                    {
                        db.Tables.Select(t => new { TableName = t.Name, Count = t.GetCount() }).ToTableString(Console.Out);
                        LineHandled(db);
                        continue;
                    }

                    var split = line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (split.Length >= 2 && split[0].EqualsIgnoreCase("table"))
                    {
                        var starPos = split[1].IndexOf('*');
                        if (starPos < 0)
                        {
                            TableStringExtensions.ToTableString(db.GetTable(split[1]), Console.Out);
                            LineHandled(db);
                            continue;
                        }

                        var query = split[1].Substring(0, starPos).Nullify();
                        if (query == null)
                        {
                            foreach (var table in db.Tables)
                            {
                                Console.WriteLine("[" + table.Name + "]");
                                TableStringExtensions.ToTableString(table, Console.Out);
                            }
                            LineHandled(db);
                            continue;
                        }

                        foreach (var table in db.Tables.Where(t => t.Name.StartsWith(query, StringComparison.OrdinalIgnoreCase)))
                        {
                            Console.WriteLine("[" + table.Name + "]");
                            TableStringExtensions.ToTableString(table, Console.Out);
                        }
                        LineHandled(db);
                        continue;
                    }

                    if (split.Length >= 2 && (split[0].EqualsIgnoreCase("rows") || split[0].EqualsIgnoreCase("data")))
                    {
                        int maxRows = int.MaxValue;
                        if (split.Length >= 3 && int.TryParse(split[2], NumberStyles.Integer, CultureInfo.CurrentCulture, out int i))
                        {
                            maxRows = i;
                        }

                        var starPos = split[1].IndexOf('*');
                        if (starPos < 0)
                        {
                            TableStringExtensions.ToTableString(db.GetTable(split[1])?.GetRows(maxRows), Console.Out);
                            LineHandled(db);
                            continue;
                        }

                        var query = split[1].Substring(0, starPos).Nullify();
                        if (query == null)
                        {
                            foreach (var table in db.Tables)
                            {
                                Console.WriteLine("[" + table.Name + "]");
                                TableStringExtensions.ToTableString(table.GetRows(maxRows), Console.Out);
                            }
                            LineHandled(db);
                            continue;
                        }

                        foreach (var table in db.Tables.Where(t => t.Name.StartsWith(query, StringComparison.OrdinalIgnoreCase)))
                        {
                            Console.WriteLine("[" + table.Name + "]");
                            TableStringExtensions.ToTableString(table.GetRows(maxRows), Console.Out);
                        }
                        LineHandled(db);
                        continue;
                    }

                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    try
                    {
                        db.LoadRows(line).ToTableString(Console.Out);
                    }
                    catch (SQLiteException sx)
                    {
                        Write(TraceLevel.Error, sx.Message);
                    }
                    LineHandled(db);
                }
                while (true);
            }
        }
    }
}
