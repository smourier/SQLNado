using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using SqlNado.Cli;

namespace SqlNado.Converter
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
            Console.WriteLine("SqlNado.Converter - " + (IntPtr.Size == 8 ? "64" : "32") +  "bit - Copyright © 2016-" + DateTime.Now.Year + " Simon Mourier. All rights reserved.");
            Console.WriteLine();
            if (CommandLine.HelpRequested || args.Length < 2)
            {
                Help();
                return;
            }

            var converter = new DatabaseConverter(args[0], args[1]);
            converter.Convert(Console.Out);
        }

        static void Help()
        {
            Console.WriteLine(Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " <input connection string> <input provider name>");
            Console.WriteLine();
            Console.WriteLine("Description:");
            Console.WriteLine("    Converts a database schema into SqlNado-compatible C# code.");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("    " + Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " \"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\\mypath1\\nw.mdb\" System.Data.OleDb");
            Console.WriteLine("    Converts Access JET 4 nw.mdb database into SqlNado-compatible C# code.");
            Console.WriteLine();
            Console.WriteLine("    " + Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " \"Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\mypath1\\nw.accdb\" System.Data.OleDb");
            Console.WriteLine("    Converts Access 2007+ nw.accdb database into SqlNado-compatible C# code.");
            Console.WriteLine("    Note: make sure the Access provider is installed in the same bitness as this program.");
            Console.WriteLine();
        }
    }
}
