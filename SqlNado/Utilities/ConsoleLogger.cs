using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace SqlNado.Utilities
{
    public class ConsoleLogger : ISQLiteLogger
    {
        public ConsoleLogger()
            : this(true)
        {
        }

        public ConsoleLogger(bool addThreadId)
        {
            AddThreadId = addThreadId;
        }

        public bool AddThreadId { get; set; }

        public virtual void Log(TraceLevel level, object value, [CallerMemberName] string methodName = null)
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

            string tid = AddThreadId ? "[" + Environment.CurrentManagedThreadId + "]:" : null;

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
}
