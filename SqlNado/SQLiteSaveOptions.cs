using System;
using System.Text;

namespace SqlNado
{
    public class SQLiteSaveOptions
    {
        public SQLiteSaveOptions(SQLiteDatabase database)
        {
            if (database == null)
                throw new ArgumentNullException(nameof(database));

            Database = database;
            Index = -1;
            UseTransactionForSchemaSynchronization = true;
        }

        public SQLiteDatabase Database { get; }
        public virtual bool SynchronizeSchema { get; set; }
        public virtual bool SynchronizeIndices { get; set; }
        public virtual bool DeleteUnusedColumns { get; set; }
        public virtual bool ObjectEventsDisabled { get; set; }
        public virtual SQLiteConflictResolution ConflictResolution { get; set; }
        public virtual bool UseTransaction { get; set; }
        public virtual bool UseTransactionForSchemaSynchronization { get; set; }
        public virtual bool UseSavePoint { get; set; }
        public virtual bool DontTryUpdate { get; set; }
        public virtual Func<SQLiteObjectColumn, object, object?>? GetValueForBindFunc { get; set; }
        public virtual string? SavePointName { get; protected internal set; }
        public virtual int Index { get; protected internal set; }
        public virtual SQLiteBuildTableOptions? BuildTableOptions { get; set; }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("SynchronizeSchema=" + SynchronizeSchema);
            sb.AppendLine("SynchronizeIndices=" + SynchronizeIndices);
            sb.AppendLine("DeleteUnusedColumns=" + DeleteUnusedColumns);
            sb.AppendLine("ObjectEventsDisabled=" + ObjectEventsDisabled);
            sb.AppendLine("ConflictResolution=" + ConflictResolution);
            return sb.ToString();
        }
    }
}
