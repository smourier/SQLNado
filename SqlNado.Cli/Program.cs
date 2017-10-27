using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using SqlNado.Utilities;

namespace SqlNado.Cli
{
    class Program
    {
        static void Main(string[] args)
        {
            if (Debugger.IsAttached)
            {
                SafeMain(args);
            }
            else
            {
                try
                {
                    SafeMain(args);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }
        }

        static void SafeMain(string[] args)
        {
            Console.WriteLine("SqlNado.Cli - Copyright © 2016-" + DateTime.Now.Year + " Simon Mourier. All rights reserved.");
            Console.WriteLine();
            if (CommandLine.HelpRequested || args.Length < 1)
            {
                Help();
                return;
            }

            var traces = CommandLine.GetArgument("traces", false);
            Console.CancelKeyPress += (sender, e) => e.Cancel = true;
            string path = Path.GetFullPath(args[0]);
            Console.WriteLine("Path: " + path);
            Console.WriteLine();
            using (var db = new SQLiteDatabase(path))
            {
                if (traces)
                {
                    db.Logger = new ConsoleLogger(false);
                }

                do
                {
                    var line = Console.ReadLine();
                    if (line == null)
                        break;

                    if (line.EqualsIgnoreCase("bye") || line.EqualsIgnoreCase("quit") ||
                        line.EqualsIgnoreCase("exit") || line.EqualsIgnoreCase("q"))
                        break;

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

            Console.WriteLine();
            Console.WriteLine("Exited.");
        }

        static void Help()
        {
            Console.WriteLine(Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " <database file path>");
            Console.WriteLine();
            Console.WriteLine("Description:");
            Console.WriteLine("    Opens the interactive command-line interpreter on the specified database. Only SQLite V3 databases are supported.");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("    " + Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " c:\\mypath\\myfile.db");
            Console.WriteLine("    Opens the interactive command-line interpreter on the c:\\mypath\\myfile.db SQLite database.");
            Console.WriteLine();
        }
    }
}
