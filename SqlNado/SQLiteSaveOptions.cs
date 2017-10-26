namespace SqlNado
{
    public class SQLiteSaveOptions
    {
        public virtual bool SynchronizeSchema { get; set; }
        public virtual bool DeleteUnusedColumns { get; set; }
        public virtual bool ObjectEventsDisabled { get; set; }
        public virtual SQLiteConflictResolution ConflictResolution { get; set; }
    }
}
