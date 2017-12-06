using System;
using System.Diagnostics;
using System.Linq;

namespace SqlNado.Utilities
{
    public class InteractiveShell : InteractiveShell<SQLiteDatabase>
    {
    }

    public class InteractiveShell<T> where T : SQLiteDatabase
    {
        public ISQLiteLogger Logger { get; set; }

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

                    if (line.EqualsIgnoreCase("clear") || line.EqualsIgnoreCase("cls"))
                    {
                        Console.Clear();
                        continue;
                    }

                    if (line.EqualsIgnoreCase("this"))
                    {
                        TableStringExtensions.ToTableString(db, Console.Out);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("tables"))
                    {
                        db.Tables.Select(t => new { t.Name, t.RootPage, t.Sql }).ToTableString(Console.Out);
                        continue;
                    }

                    if (line.EqualsIgnoreCase("stats"))
                    {
                        db.Tables.Select(t => new { TableName = t.Name, Count = t.GetCount() }).ToTableString(Console.Out);
                        continue;
                    }

                    var split = line.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (split.Length >= 2 && split[0].EqualsIgnoreCase("table"))
                    {
                        int starPos = split[1].IndexOf('*');
                        if (starPos < 0)
                        {
                            TableStringExtensions.ToTableString(db.GetTable(split[1]), Console.Out);
                            continue;
                        }

                        string query = split[1].Substring(0, starPos).Nullify();
                        if (query == null)
                        {
                            foreach (var table in db.Tables)
                            {
                                Console.WriteLine("[" + table.Name + "]");
                                TableStringExtensions.ToTableString(table, Console.Out);
                            }
                            continue;
                        }

                        foreach (var table in db.Tables.Where(t => t.Name.StartsWith(query, StringComparison.OrdinalIgnoreCase)))
                        {
                            Console.WriteLine("[" + table.Name + "]");
                            TableStringExtensions.ToTableString(table, Console.Out);
                        }
                        continue;
                    }

                    if (split.Length >= 2 && (split[0].EqualsIgnoreCase("rows") || split[0].EqualsIgnoreCase("data")))
                    {
                        int maxRows = int.MaxValue;
                        if (split.Length >= 3 && int.TryParse(split[2], out int i))
                        {
                            maxRows = i;
                        }

                        int starPos = split[1].IndexOf('*');
                        if (starPos < 0)
                        {
                            TableStringExtensions.ToTableString(db.GetTable(split[1])?.GetRows(maxRows), Console.Out);
                            continue;
                        }

                        string query = split[1].Substring(0, starPos).Nullify();
                        if (query == null)
                        {
                            foreach (var table in db.Tables)
                            {
                                Console.WriteLine("[" + table.Name + "]");
                                TableStringExtensions.ToTableString(table.GetRows(maxRows), Console.Out);
                            }
                            continue;
                        }

                        foreach (var table in db.Tables.Where(t => t.Name.StartsWith(query, StringComparison.OrdinalIgnoreCase)))
                        {
                            Console.WriteLine("[" + table.Name + "]");
                            TableStringExtensions.ToTableString(table.GetRows(maxRows), Console.Out);
                        }
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
