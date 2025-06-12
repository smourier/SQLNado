namespace SqlNado.Utilities;

public class ConsoleLogger(bool addThreadId) : ISQLiteLogger
{
    public ConsoleLogger()
        : this(true)
    {
    }

    public bool AddThreadId { get; set; } = addThreadId;

    public virtual void Log(TraceLevel level, object value, [CallerMemberName] string? methodName = null)
    {
        switch (level)
        {
            case TraceLevel.Error:
                Console.ForegroundColor = ConsoleColor.Red;
                break;

            case TraceLevel.Warning:
                Console.ForegroundColor = ConsoleColor.Yellow;
                break;

            case TraceLevel.Verbose:
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                break;

            case TraceLevel.Off:
                return;
        }

        var tid = AddThreadId ? "[" + Environment.CurrentManagedThreadId + "]:" : null;

        if (!string.IsNullOrWhiteSpace(methodName))
        {
            Console.WriteLine(tid + methodName + ": " + value);
        }
        else
        {
            Console.WriteLine(tid + value);
        }
        Console.ResetColor();
    }
}
