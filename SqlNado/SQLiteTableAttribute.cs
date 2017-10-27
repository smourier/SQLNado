using System;

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false)]
    public class SQLiteTableAttribute : Attribute
    {
        public virtual string Name { get; set; }

        // note every WITHOUT ROWID table must have a PRIMARY KEY
        public virtual bool WithoutRowId { get; set; }

        public override string ToString() => Name;
    }
}
