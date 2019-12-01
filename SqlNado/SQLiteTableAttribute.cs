using System;

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false)]
    public sealed class SQLiteTableAttribute : Attribute
    {
        public string Name { get; set; }
        public string Schema { get; set; } // unused in SqlNado's SQLite
        public string Module { get; set; } // virtual table
        public string ModuleArguments { get; set; } // virtual table

        // note every WITHOUT ROWID table must have a PRIMARY KEY
        public bool WithoutRowId { get; set; }

        public override string ToString() => Name;
    }
}
