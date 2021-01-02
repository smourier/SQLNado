using System;
using System.Globalization;
using System.Runtime.Serialization;

namespace SqlNado
{
    [Serializable]
    public class SqlNadoException : Exception
    {
        public const string Prefix = "SQN";

        public SqlNadoException()
            : base(Prefix + "0001: SqlNado exception.")
        {
        }

        public SqlNadoException(string message)
            : base(Prefix + ":"+ message)
        {
        }

        public SqlNadoException(Exception innerException)
            : base(null, innerException)
        {
        }

        public SqlNadoException(string message, Exception innerException)
            : base(Prefix + ":" + message, innerException)
        {
        }

        protected SqlNadoException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public int Code => GetCode(Message);

        public static int GetCode(string message)
        {
            if (message == null)
                return -1;

            if (!message.StartsWith(Prefix, StringComparison.Ordinal))
                return -1;

            var pos = message.IndexOf(':', Prefix.Length);
            if (pos < 0)
                return -1;

            if (int.TryParse(message.Substring(Prefix.Length, pos - Prefix.Length), NumberStyles.Integer, CultureInfo.InvariantCulture, out int i))
                return i;

            return -1;
        }
    }
}
