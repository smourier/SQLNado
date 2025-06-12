using System;
using SqlNado.Utilities;

namespace SqlNado.Temp
{
    public static class DocProgram
    {
        public static void Starter()
        {
            using var db = new SQLiteDatabase("my.db");
            var customer = new Customer();
            customer.Email = "kilroy@example.com";
            customer.Name = "Kilroy";

            // update or insert (using the primary key)
            db.Save(customer);

            // dumps the customer list to the console
            db.LoadAll<Customer>().ToTableString(Console.Out);

            // dumps the sql query result to the console (should be the same as previous)
            db.LoadRows("SELECT * FROM Customer").ToTableString(Console.Out);

            // dumps the Customer table schema to the console
            TableStringExtensions.ToTableString(db.GetTable<Customer>(), Console.Out);

            // dumps the Customer table columns definitions to the console
            db.GetTable<Customer>().Columns.ToTableString(Console.Out);
        }

        public class Customer
        {
            [SQLiteColumn(IsPrimaryKey = true)]
            public string Email { get; set; }
            public string Name { get; set; }
        }
    }
}
