using System;
using System.Diagnostics;
using System.Reflection;
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
            bool nologo = CommandLine.GetArgument("nologo", false);
            if (!nologo)
            {
                Console.WriteLine("SqlNado.Converter - " + (IntPtr.Size == 8 ? "64" : "32") + "bit - Copyright © 2016-" + DateTime.Now.Year + " Simon Mourier. All rights reserved.");
                Console.WriteLine();
            }

            if (CommandLine.HelpRequested || args.Length < 2)
            {
                Help();
                return;
            }

            var converter = new DatabaseConverter(args[0], args[1]);
            converter.Options = CommandLine.GetArgument("options", DatabaseConverterOptions.None);
            converter.Namespace = CommandLine.GetNullifiedArgument("ns");
            converter.Convert(Console.Out);
        }

        static void Help()
        {
            Console.WriteLine(Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " <input connection string> <input provider name> [optional parameters]");
            Console.WriteLine();
            Console.WriteLine("Description:");
            Console.WriteLine("    Converts a database schema into SqlNado-compatible C# code.");
            Console.WriteLine();
            Console.WriteLine("Optional Parameters:");
            Console.WriteLine("    /options:<flags>                 Options for output.");
            Console.WriteLine("        0: None                          No option (default value).");
            Console.WriteLine("        1: DeriveFromBaseObject          Generated C# classes derive from SQLiteBaseObject.");
            Console.WriteLine("        2: KeepRowguid                   Keep rowguid columns. By default they are removed. SQL Server provider only.");
            Console.WriteLine("        4: AddNamespaceAndUsings         Adds surrounding namespace and required usings.");
            Console.WriteLine();
            Console.WriteLine("    /nologo                          Do not display the header logo text.");
            Console.WriteLine("    /ns:<namespace>                  Namespace name to generate. Requires options AddNamespaceAndUsings.");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("    " + Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " \"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=C:\\mypath1\\nw.mdb\" System.Data.OleDb");
            Console.WriteLine("    Converts Access JET 4 nw.mdb database into SqlNado-compatible C# code.");
            Console.WriteLine();
            Console.WriteLine("    " + Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " \"Provider=Microsoft.ACE.OLEDB.12.0;Data Source=C:\\mypath1\\nw.accdb\" System.Data.OleDb");
            Console.WriteLine("    Converts Access 2007+ nw.accdb database into SqlNado-compatible C# code.");
            Console.WriteLine("    Note: make sure the Access provider is installed in the same bitness as this program.");
            Console.WriteLine();
            Console.WriteLine("    " + Assembly.GetEntryAssembly().GetName().Name.ToUpperInvariant() + " \"Server=myServer;Database=myDataBase;Trusted_Connection=True;\" SqlServer");
            Console.WriteLine("    Converts SQL Server 'myDataBase' database into SqlNado-compatible C# code.");
            Console.WriteLine();
        }
    }
}
