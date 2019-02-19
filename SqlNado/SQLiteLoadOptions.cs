using System;
using System.Text;

namespace SqlNado
{
    public class SQLiteLoadOptions
    {
        public SQLiteLoadOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
        }

        public SQLiteDatabase Database { get; }
        public virtual bool ObjectEventsDisabled { get; set; }
        public virtual bool ObjectChangeEventsDisabled { get; set; }
        public virtual bool CreateIfNotLoaded { get; set; }
        public virtual bool DontConvertPrimaryKey { get; set; }
        public virtual int MaximumRows { get; set; }
        public virtual int Limit { get; set; }
        public virtual int Offset { get; set; }
        public virtual bool RemoveDuplicates { get; set; }
        public virtual bool TestTableExists { get; set; }
        public virtual Func<Type, SQLiteStatement, SQLiteLoadOptions, object> GetInstanceFunc { get; set; }
        public virtual Func<SQLiteError, SQLiteOnErrorAction> ErrorHandler { get; set; }

        public virtual bool TryChangeType(object input, Type conversionType, out object value) => Database.TryChangeType(input, conversionType, out value);

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("CreateIfNotLoaded=" + CreateIfNotLoaded);
            sb.AppendLine("DontConvertPrimaryKey=" + DontConvertPrimaryKey);
            sb.AppendLine("Limit=" + Limit);
            sb.AppendLine("ObjectEventsDisabled=" + ObjectEventsDisabled);
            sb.AppendLine("ObjectChangeEventsDisabled=" + ObjectChangeEventsDisabled);
            sb.AppendLine("Offset=" + Offset);
            sb.AppendLine("MaximumRows=" + MaximumRows);
            sb.AppendLine("RemoveDuplicates=" + RemoveDuplicates);
            sb.AppendLine("TestTableExists=" + TestTableExists);
            return sb.ToString();
        }
    }
}
