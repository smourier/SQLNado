using System;
using System.Linq;

namespace SqlNado.Utilities
{
    public class InteractiveShell
    {
        public ISQLiteLogger Logger { get; set; }

        protected virtual bool HandleLine(string line) => false;

        public virtual void Run(string filePath)
        {
            using (var db = new SQLiteDatabase(filePath))
            {
                db.Logger = Logger;
                do
                {
                    var line = Console.ReadLine();
                    if (line == null)
                        break;

                    if (line.EqualsIgnoreCase("bye") || line.EqualsIgnoreCase("quit") ||
                        line.EqualsIgnoreCase("exit") || line.EqualsIgnoreCase("q"))
                        break;

                    if (HandleLine(line))
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
                        db.Tables.Select(t => new { Name = t.Name, RootPage = t.RootPage, Sql = t.Sql }).ToTableString(Console.Out);
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
                        Console.ForegroundColor = ConsoleColor.Red;
                        try
                        {
                            Console.WriteLine(sx.Message);
                        }
                        finally
                        {
                            Console.ResetColor();
                        }
                    }
                }
                while (true);
            }
        }
    }
}
