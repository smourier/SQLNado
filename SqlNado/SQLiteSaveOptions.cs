namespace SqlNado
{
    public class SQLiteSaveOptions
    {
        public bool SynchronizeSchema { get; set; }
        public bool DeleteUnusedColumns { get; set; }
        public bool InsertOnly { get; set; }
        public bool UpdateOnly { get; set; }
        public bool ObjectEventsDisabled { get; set; }
    }
}
