using System;
using System.Runtime.Serialization;

namespace SqlNado
{
    [Serializable]
    public class SQLiteException : Exception
    {
        public SQLiteException()
            : this(SQLiteErrorCode.SQLITE_ERROR)
        {
        }

        public SQLiteException(SQLiteErrorCode code)
            : base(GetMessage(code))
        {
            Code = code;
        }

        internal SQLiteException(SQLiteErrorCode code, string message)
            : base(GetMessage(code, message))
        {
            Code = code;
        }

        public SQLiteException(string message)
            : base(message)
        {
        }

        public SQLiteException(Exception innerException)
            : base(null, innerException)
        {
        }

        public SQLiteException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        protected SQLiteException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public SQLiteErrorCode Code { get; }

        private static string GetMessage(SQLiteErrorCode code, string message)
        {
            var msg = GetMessage(code);
            if (!string.IsNullOrEmpty(message))
            {
                msg += " " + char.ToUpperInvariant(message[0]) + message.Substring(1);
                if (!msg.EndsWith(".", StringComparison.Ordinal))
                {
                    msg += ".";
                }
            }
            return msg;
        }

        public static string GetMessage(SQLiteErrorCode code)
        {
            string? msg = null;
            switch (code)
            {
                case SQLiteErrorCode.SQLITE_ERROR:
                    msg = "SQL error or missing database";
                    break;

                case SQLiteErrorCode.SQLITE_INTERNAL:
                    msg = "Internal malfunction";
                    break;

                case SQLiteErrorCode.SQLITE_PERM:
                    msg = "Access permission denied";
                    break;

                case SQLiteErrorCode.SQLITE_ABORT:
                    msg = "Callback routine requested an abort";
                    break;

                case SQLiteErrorCode.SQLITE_BUSY:
                    msg = "The database file is locked";
                    break;

                case SQLiteErrorCode.SQLITE_LOCKED:
                    msg = "A table in the database is locked";
                    break;

                case SQLiteErrorCode.SQLITE_NOMEM:
                    msg = "A malloc() failed";
                    break;

                case SQLiteErrorCode.SQLITE_READONLY:
                    msg = "Attempt to write a readonly database";
                    break;

                case SQLiteErrorCode.SQLITE_INTERRUPT:
                    msg = "Operation terminated by sqlite3_interrupt()";
                    break;

                case SQLiteErrorCode.SQLITE_IOERR:
                    msg = "Some kind of disk I/O error occurred";
                    break;

                case SQLiteErrorCode.SQLITE_CORRUPT:
                    msg = "The database disk image is malformed";
                    break;

                case SQLiteErrorCode.SQLITE_NOTFOUND:
                    msg = "Unknown opcode in sqlite3_file_control()";
                    break;

                case SQLiteErrorCode.SQLITE_FULL:
                    msg = "Insertion failed because database is full";
                    break;

                case SQLiteErrorCode.SQLITE_CANTOPEN:
                    msg = "Unable to open the database file";
                    break;

                case SQLiteErrorCode.SQLITE_PROTOCOL:
                    msg = "Database lock protocol error";
                    break;

                case SQLiteErrorCode.SQLITE_EMPTY:
                    msg = "Database is empty";
                    break;

                case SQLiteErrorCode.SQLITE_SCHEMA:
                    msg = "The database schema changed";
                    break;

                case SQLiteErrorCode.SQLITE_TOOBIG:
                    msg = "String or BLOB exceeds size limit";
                    break;

                case SQLiteErrorCode.SQLITE_CONSTRAINT:
                    msg = "Abort due to constraint violation";
                    break;

                case SQLiteErrorCode.SQLITE_MISMATCH:
                    msg = "Data type mismatch";
                    break;

                case SQLiteErrorCode.SQLITE_MISUSE:
                    msg = "Library used incorrectly";
                    break;

                case SQLiteErrorCode.SQLITE_NOLFS:
                    msg = "Uses OS features not supported on host";
                    break;

                case SQLiteErrorCode.SQLITE_AUTH:
                    msg = "Authorization denied";
                    break;

                case SQLiteErrorCode.SQLITE_FORMAT:
                    msg = "Auxiliary database format error";
                    break;

                case SQLiteErrorCode.SQLITE_RANGE:
                    msg = "2nd parameter to sqlite3_bind out of range";
                    break;

                case SQLiteErrorCode.SQLITE_NOTADB:
                    msg = "File opened that is not a database file";
                    break;

                case SQLiteErrorCode.SQLITE_ROW:
                    msg = "sqlite3_step() has another row ready";
                    break;

                case SQLiteErrorCode.SQLITE_DONE:
                    msg = "sqlite3_step() has finished executing";
                    break;
            }

            var codeMsg = code.ToString() + " (" + (int)code + ")";
            return msg != null ? codeMsg + ": " + msg + "." : codeMsg;
        }
    }
}
