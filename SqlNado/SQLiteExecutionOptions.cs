namespace SqlNado;

public class SQLiteExecutionOptions
{
    public static SQLiteExecutionOptions NoStatementsCache { get; } = new ReadOnlyExecutionOptions();

    // null  = follow the database default (SQLiteDatabase.EnableStatementsCache),
    // false = bypass the cache
    // true  = force use the cache
    public virtual bool? UseStatementsCache { get; set; }
    public virtual Func<SQLiteError, SQLiteOnErrorAction>? ErrorHandler { get; set; }

    private sealed class ReadOnlyExecutionOptions : SQLiteExecutionOptions
    {
        public ReadOnlyExecutionOptions()
        {
            base.UseStatementsCache = false;
        }

        public override bool? UseStatementsCache { get => base.UseStatementsCache; set => throw new InvalidOperationException(); }
        public override Func<SQLiteError, SQLiteOnErrorAction>? ErrorHandler { get => base.ErrorHandler; set => throw new InvalidOperationException(); }
    }
}
