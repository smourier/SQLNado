using System;

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false)]
    public class SQLiteTableAttribute : Attribute
    {
        public virtual string Name { get; set; }
        public virtual bool DisableRowId { get; set; }

        public override string ToString() => Name;
    }
}
