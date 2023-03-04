using System;

namespace SqlNado.Temp
{
    class Program
    {
        static void Main(string[] args)
        {
            // this project exists solely to be able to validate nullable in .NET core context
            // which are more restrictive than in netstandard contexts
            using (var db = new SQLiteDatabase(":memory:"))
            {
                Console.WriteLine(SQLiteDatabase.NativeDllPath);
            }
        }
    }
}
