using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using SqlNado.Utilities;

namespace SqlNado.Temp
{
    class Program
    {
        static void Main(string[] args)
        {
            if (Debugger.IsAttached)
            {
                SafeMain(args);
                return;
            }

            try
            {
                SafeMain(args);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        static void SafeMain(string[] args)
        {
            using (var db = new SQLiteDatabase("chinook.db"))
            {
                Console.WriteLine(db.FilePath);
                var value = db.Execute("SELECT name, rootpage, sql FROM sqlite_master WHERE type='table'");
                //var value = db.Execute("SELECT firstname, lastname FROM customers");

                //TableString.DefaultMaximumWidth = 120;
                for (int i = 0; i < TableString.DefaultMaximumWidth - 1; i++)
                {
                    if ((i % 10) == 0)
                    {
                        Console.Write((i / 10) % 10);
                    }
                    else
                    {
                        Console.Write(' ');
                    }
                }
                Console.WriteLine();
                for (int i = 0; i < TableString.DefaultMaximumWidth - 1; i++)
                {
                    Console.Write(i % 10);
                }
                Console.WriteLine();
                value.ToTableString(Console.Out);

                var x = new { Test = new string('x', 100) };
                Console.WriteLine(TableStringExtensions.ToTableString(10, x));
                //TableStringExtensions.ToTableString(10, x, Console.Out);
                TableStringExtensions.ToTableString(0, x, Console.Out);
                return;
                Console.WriteLine(TableStringExtensions.ToTableString(10, db));

                var dic = new Dictionary<string, object>();
                dic.Add("thing", "stuff");
                dic.Add("stuff", DateTime.Now);
                dic.ToTableString(Console.Out);
            }
        }
    }
}
