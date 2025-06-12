namespace SqlNado;

public class SQLiteSaveOptions(SQLiteDatabase database)
{
    public SQLiteDatabase Database { get; } = database ?? throw new ArgumentNullException(nameof(database));
    public virtual bool SynchronizeSchema { get; set; }
    public virtual bool SynchronizeIndices { get; set; }
    public virtual bool DeleteUnusedColumns { get; set; }
    public virtual bool ObjectEventsDisabled { get; set; }
    public virtual SQLiteConflictResolution ConflictResolution { get; set; }
    public virtual bool UseTransaction { get; set; }
    public virtual bool UseTransactionForSchemaSynchronization { get; set; } = true;
    public virtual bool UseSavePoint { get; set; }
    public virtual bool DontTryUpdate { get; set; }
    public virtual Func<SQLiteObjectColumn, object, object?>? GetValueForBindFunc { get; set; }
    public virtual string? SavePointName { get; protected internal set; }
    public virtual int Index { get; protected internal set; } = -1;
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
