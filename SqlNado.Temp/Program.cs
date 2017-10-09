using System;
using System.Diagnostics;
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
            //for (int i = 0; i < 128; i++)
            //{
            //    Console.WriteLine(i.ToString("X4") + ":" + (char)i);
            //}
            //return;
            using (var db = new SQLiteDatabase("chinook.db"))
            {
                Console.WriteLine(db.FilePath);
                var value = db.Execute("SELECT name, rootpage, sql FROM sqlite_master WHERE type='table'");
                //var value = db.Execute("SELECT firstname, lastname FROM customers");
                value.ToTableString(Console.Out);

                //TableStringExtensions.ToTableString(10, db, Console.Out);
                Console.WriteLine(TableStringExtensions.ToTableString(10, db));
            }
        }
    }
}
