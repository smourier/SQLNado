using System;
using System.Linq;
using SqlNado.Utilities;

namespace SqlNado
{
    public class SQLiteType
    {
        public static readonly SQLiteType ObjectToString;

        static SQLiteType()
        {
            Func<SQLiteBindContext, SQLiteErrorCode> bindFunc = (ctx) =>
            {
                Conversions.TryChangeType(ctx.Value, ctx.FormatProvider, out string text); // always succeeds for a string
                return ctx.BindString(text);
            };
            ObjectToString = new SQLiteType(bindFunc, typeof(object));
        }

        public SQLiteType(Func<SQLiteBindContext, SQLiteErrorCode> bindFunc, params Type[] types)
        {
            if (bindFunc == null)
                throw new ArgumentNullException(nameof(bindFunc));

            if (types == null)
                throw new ArgumentNullException(nameof(types));

            if (types.Length == 0)
                throw new ArgumentException(null, nameof(types));

            foreach (var type in types)
            {
                if (type == null)
                    throw new ArgumentException(null, nameof(types));
            }

            HandledTypes = types;
            BindFunc = bindFunc;
        }

        public Type[] HandledTypes { get; }
        public virtual Func<SQLiteBindContext, SQLiteErrorCode> BindFunc { get; }

        public override string ToString() => string.Join(", ", HandledTypes.Select(t => t.FullName));
    }
}
