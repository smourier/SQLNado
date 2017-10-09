using System;
using System.CodeDom.Compiler;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SqlNado.Utilities
{
    public class TableString
    {
        private List<ColumnString> _columns = new List<ColumnString>();

        public TableString()
        {
            IndentTabString = " ";
        }

        public virtual void AddColumn(ColumnString column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            column.Index = _columns.Count;
            _columns.Add(column);
        }

        public string IndentTabString { get; set; }
        public virtual IReadOnlyList<ColumnString> Columns => _columns;

        protected virtual void CreateColumns(object first)
        {
            if (first == null)
                throw new ArgumentNullException(nameof(first));

            if (first is Array array)
            {
                for (int i = 0; i < array.Length; i++)
                {
                    AddColumn(new ArrayItemColumnString(i));
                }
                return;
            }

            foreach (var property in first.GetType().GetProperties())
            {
                var browsable = property.GetCustomAttribute<BrowsableAttribute>();
                if (browsable != null && !browsable.Browsable)
                    continue;

                AddColumn(new PropertyInfoColumnString(property));
            }
        }

        public string Write(IEnumerable enumerable) => Write(0, enumerable);
        public virtual string Write(int indent, IEnumerable enumerable)
        {
            using (var sw = new StringWriter())
            {
                Write(indent, sw, enumerable);
                return sw.ToString();
            }
        }

        public string WriteObject(object obj) => WriteObject(0, obj);
        public virtual string WriteObject(int indent, object obj)
        {
            using (var sw = new StringWriter())
            {
                WriteObject(indent, sw, obj);
                return sw.ToString();
            }
        }

        public void WriteObject(TextWriter writer, object obj) => WriteObject(0, writer, obj);
        public virtual void WriteObject(int indent, TextWriter writer, object obj)
        {
            if (obj == null)
                return;

            new ObjectTableString(obj).Write(indent, writer);
        }

        public void Write(TextWriter writer, IEnumerable enumerable) => Write(0, writer, enumerable);
        public virtual void Write(int indent, TextWriter writer, IEnumerable enumerable)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            // something to write?
            if (enumerable == null)
                return;

            TextWriter wr;
            if (indent > 0)
            {
                var itw = new IndentedTextWriter(writer, IndentTabString);
                itw.Indent = indent;
                for (int i = 0; i < indent; i++)
                {
                    writer.Write(IndentTabString);
                }
                wr = itw;
            }
            else
            {
                wr = writer;
            }

            int[] columnLengths = null;

            // note: we scan one once, but we'll execute the loop twice. this is mandatory to compute lengths
            var rows = new List<string[]>();
            foreach (var row in enumerable)
            {
                if (columnLengths == null)
                {
                    // create the columns with the first non-null row that will create at least one column
                    if (row == null)
                        continue;

                    CreateColumns(row);
                    if (Columns.Count == 0)
                        continue;

                    columnLengths = Columns.Select(c => c.Name.Length).ToArray();
                }

                var rowValues = new string[Columns.Count];
                for (int i = 0; i < Columns.Count; i++)
                {
                    var toStringFunc = Columns[i].ToStringFunc ?? TableStringContext.DefaultToStringFunc;
                    var ctx = new TableStringContext(Columns[i]);
                    ctx.Row = row;
                    rowValues[i] = toStringFunc(ctx);
                    if (rowValues[i].Length > columnLengths[i])
                    {
                        columnLengths[i] = rowValues[i].Length;
                    }
                }
                rows.Add(rowValues);
            }

            // no valid columns
            if (columnLengths == null)
                return;

            var fullLine = new string('-', columnLengths.Sum() + 1 + columnLengths.Length * (2 + 1));
            var gridLine = new StringBuilder();
            wr.WriteLine(fullLine);
            wr.Write('|');
            gridLine.Append('|');
            for (int i = 0; i < Columns.Count; i++)
            {
                var fmt = string.Format(" {0," + columnLengths[i] + "} |", Columns[i].Name);
                wr.Write(fmt);
                gridLine.Append(new string('-', columnLengths[i] + 2) + '|');
            }

            wr.WriteLine();
            wr.WriteLine(fullLine);
            for (int r = 0; r < rows.Count; r++)
            {
                var rowValues = rows[r];
                wr.Write('|');
                for (int i = 0; i < Columns.Count; i++)
                {
                    var fmt = string.Format(" {0," + columnLengths[i] + "} |", rowValues[i]);
                    wr.Write(fmt);
                }
                wr.WriteLine();
            }
            wr.WriteLine(fullLine);
        }
    }

    public class TableStringContext
    {
        public const int MaxDefault = 50;

        public static readonly Func<TableStringContext, string> DefaultToStringFunc = (c) =>
        {
            var value = c.GetValue();
            if (value is string s)
                return c.FinalizeString(s);

            if (value is byte[] bytes)
            {
                int max = c.Column.MaxLength;
                if (max <= 0)
                {
                    max = MaxDefault;
                }

                if (bytes.Length > (max - 1) / 2)
                    return "0x" + BitConverter.ToString(bytes, 0, (max - 1) / 2).Replace("-", string.Empty) + c.Hyphens + " (" + bytes.Length + ")";

                return "0x" + BitConverter.ToString(bytes, 0, Math.Min((max - 1) / 2, bytes.Length)).Replace("-", string.Empty);
            }

            return c.FinalizeString(string.Format("{0}", value));
        };

        public TableStringContext(ColumnString column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            Column = column;
            NewLineReplacement = "\u001A";
            Hyphens = "...";
        }

        public ColumnString Column { get; }
        public virtual object Row { get; set; }
        public string NewLineReplacement { get; set; }
        public string Hyphens { get; set; }

        public virtual object GetValue() => Column.GetValueFunc(this);

        public virtual string FinalizeString(string str)
        {
            if (str == null)
                return str;

            int max = Column.MaxLength;
            if (max <= 0)
            {
                max = MaxDefault;
            }

            if (NewLineReplacement != null) // can be empty string
            {
                str = str.Replace(Environment.NewLine, NewLineReplacement);
            }

            return str.Length < max ? str : str.Substring(0, max) + Hyphens;
        }
    }

    public class ColumnString
    {
        public ColumnString(string name, Func<TableStringContext, object> getValueFunc)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (getValueFunc == null)
                throw new ArgumentNullException(nameof(getValueFunc));

            GetValueFunc = getValueFunc;
            Name = name;
        }

        public string Name { get; }
        public Func<TableStringContext, object> GetValueFunc { get; }
        public int Index { get; internal set; }
        public int MaxLength { get; set; }
        public Func<TableStringContext, string> ToStringFunc { get; set; }
    }

    public class ObjectTableString : TableString
    {
        public ObjectTableString(object obj)
        {
            if (obj == null)
                throw new ArgumentNullException(nameof(obj));

            Object = obj;
        }

        public object Object { get; }

        private IEnumerable<Tuple<string, object>> Values
        {
            get
            {
                foreach (var property in Object.GetType().GetProperties())
                {
                    if (!property.CanRead)
                        continue;

                    object value;
                    try
                    {
                        value = property.GetValue(Object);
                    }
                    catch (Exception e)
                    {
                        value = "* ERROR * " + e.Message;
                    }
                    yield return new Tuple<string, object>(property.Name, value);
                }
            }
        }

        protected override void CreateColumns(object first)
        {
            AddColumn(new ColumnString("Name", (c) => ((Tuple<string, object>)c.Row).Item1));
            AddColumn(new ColumnString("Value", (c) => ((Tuple<string, object>)c.Row).Item2));
        }

        public void Write(int indent, TextWriter writer) => Write(indent, writer, Values);
    }

    public class ArrayItemColumnString : ColumnString
    {
        public ArrayItemColumnString(int index)
            : base("#" + index.ToString(), (c) => ((Array)c.Row).GetValue(((ArrayItemColumnString)c.Column).ArrayIndex))
        {
            ArrayIndex = index;
        }

        public int ArrayIndex { get; } // could be different from column's index
    }

    public class PropertyInfoColumnString : ColumnString
    {
        // yes, performance could be inproved (delegates, etc.)
        public PropertyInfoColumnString(PropertyInfo property)
            : base(property?.Name, (c) => ((PropertyInfoColumnString)c.Column).Property.GetValue(c.Row))
        {
            Property = property;
        }

        public PropertyInfo Property { get; }
    }

    public static class TableStringExtensions
    {
        private static readonly TableString _instance = new TableString();

        public static string ToTableString<T>(this IEnumerable<T> enumerable) => _instance.Write(enumerable);
        public static string ToTableString(this IEnumerable enumerable) => _instance.Write(enumerable);

        public static void ToTableString<T>(this IEnumerable<T> enumerable, TextWriter writer) => _instance.Write(writer, enumerable);
        public static void ToTableString(this IEnumerable enumerable, TextWriter writer) => _instance.Write(writer, enumerable);

        public static string ToTableString(int indent, object obj) => _instance.WriteObject(indent, obj);
        public static string ToTableString(object obj) => _instance.WriteObject(obj);
        public static void ToTableString(int indent, object obj, TextWriter writer) => _instance.WriteObject(indent, writer, obj);
        public static void ToTableString(object obj, TextWriter writer) => _instance.WriteObject(writer, obj);
    }
}
