using System;

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false)]
    public class SQLiteTableAttribute : Attribute
    {
        public virtual string Name { get; set; }
        public virtual string Schema { get; set; } // unused in SqlNado's SQLite
        public virtual string Module { get; set; } // virtual table
        public virtual string ModuleArguments { get; set; } // virtual table

        // note every WITHOUT ROWID table must have a PRIMARY KEY
        public virtual bool WithoutRowId { get; set; }

        public override string ToString() => Name;
    }
}
