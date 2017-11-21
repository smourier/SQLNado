using System;
using System.Diagnostics;
using System.IO;
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
            var shell = new InteractiveShell();
            if (traces)
            {
                shell.Logger = new ConsoleLogger(true);
            }
            shell.Run(path);
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
