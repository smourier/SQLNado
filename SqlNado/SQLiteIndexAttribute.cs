using System;
using System.Collections.Generic;

namespace SqlNado
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
    public class SQLiteIndexAttribute : Attribute
    {
        public const int DefaultOrder = -1;

        public SQLiteIndexAttribute(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException(null, nameof(name));

            Order = DefaultOrder;
            Name = name;
        }

        public virtual string Name { get; }
        public virtual string SchemaName { get; set; }
        public virtual bool IsUnique { get; set; }
        public virtual int Order { get; set; }
        public virtual string CollationName { get; set; }
        public virtual SQLiteDirection? Direction { get; set; }

        public override string ToString()
        {
            string s = Name + ":" + Order;

            var atts = new List<string>();
            if (IsUnique)
            {
                atts.Add("U");
            }

            if (!string.IsNullOrWhiteSpace(CollationName))
            {
                atts.Add("COLLATE " + CollationName);
            }

            if (Direction.HasValue)
            {
                atts.Add(Direction == SQLiteDirection.Ascending ? "ASC" : "DESC");
            }

            if (atts.Count > 0)
                return s + " (" + string.Join("", atts) + ")";

            return s;
        }
    }
}
