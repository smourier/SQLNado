using System;
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

        public virtual void AddColumn(ColumnString column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            column.Index = _columns.Count;
            _columns.Add(column);
        }

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

        public string Write(IEnumerable enumerable)
        {
            using (var sw = new StringWriter())
            {
                Write(sw, enumerable);
                return sw.ToString();
            }
        }

        public virtual void Write(TextWriter writer, IEnumerable enumerable)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));

            if (enumerable == null)
                return;

            int[] columnLengths = null;
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
                    var toStringFunc = Columns[i].ToStringFunc ?? ColumnString.DefaultToStringFunc;
                    rowValues[i] = toStringFunc(Columns[i], row);
                    if (rowValues[i].Length > columnLengths[i])
                    {
                        columnLengths[i] = rowValues[i].Length;
                    }
                }
                rows.Add(rowValues);
            }

            var fullLine = new string('-', columnLengths.Sum() + 1 + columnLengths.Length * (2 + 1));
            var gridLine = new StringBuilder();
            //var sb = new StringBuilder(fullLine);
            writer.WriteLine(fullLine);
            //sb.AppendLine();
            writer.Write('|');
            //sb.Append('|');
            gridLine.Append('|');
            for (int i = 0; i < Columns.Count; i++)
            {
                //sb.AppendFormat(" {0," + columnLengths[i] + "} |", properties[i].Name);
                var fmt = string.Format(" {0," + columnLengths[i] + "} |", Columns[i].Name);
                writer.Write(fmt);
                gridLine.Append(new string('-', columnLengths[i] + 2) + '|');
            }

            writer.WriteLine();
            //sb.AppendLine();
            writer.WriteLine();
            //sb.AppendLine(gridLine.ToString());
            for (int r = 0; r < rows.Count; r++)
            {
                var rowValues = rows[r];
                //sb.Append('|');
                writer.Write('|');
                for (int i = 0; i < Columns.Count; i++)
                {
                    //sb.AppendFormat(" {0," + columnLengths[i] + "} |", rowValues[i]);
                    var fmt = string.Format(" {0," + columnLengths[i] + "} |", rowValues[i]);
                    writer.Write(fmt);
                }
                //sb.AppendLine();
                writer.WriteLine();
            }
            //sb.Append(fullLine);
            writer.Write(fullLine);
            //return sb.ToString();
        }
    }

    public abstract class ColumnString
    {
        public static readonly Func<ColumnString, object, string> DefaultToStringFunc = (c, o) =>
        {
            const int max = 50;
            var value = c.GetValue(o);
            if (value is string s)
                return s;

            if (value is byte[] bytes)
            {
                if (bytes.Length > (max - 1) / 2)
                    return "0x" + BitConverter.ToString(bytes, 0, (max - 1) / 2).Replace("-", string.Empty) + "... (" + bytes.Length + ")";

                return "0x" + BitConverter.ToString(bytes, 0, Math.Min((max - 1) / 2, bytes.Length)).Replace("-", string.Empty);
            }

            s = string.Format("{0}", value);
            return s.Length < max ? s : s.Substring(0, max) + "...";
        };

        public ColumnString(string name)
        {
            if (name == null)
                throw new ArgumentNullException(nameof(name));

            Name = name;
        }

        public string Name { get; }
        public int Index { get; internal set; }
        public abstract object GetValue(object obj);
        public Func<ColumnString, object, string> ToStringFunc { get; set; }
    }

    public class ArrayItemColumnString : ColumnString
    {
        public ArrayItemColumnString(int index)
            : base(index.ToString())
        {
            ArrayIndex = index;
        }

        public int ArrayIndex { get; } // could be different from column's index

        public override object GetValue(object obj) => ((Array)obj).GetValue(ArrayIndex);
    }

    public class PropertyInfoColumnString : ColumnString
    {
        public PropertyInfoColumnString(PropertyInfo property)
            : base(property?.Name)
        {
            Property = property;
        }

        public PropertyInfo Property { get; }

        // yes, performance could be inproved (delegates, etc.)
        public override object GetValue(object obj) => Property.GetValue(obj);
    }

    public static class TableStringExtensions
    {
        public static string ToTableString<T>(this IEnumerable<T> enumerable) => new TableString().Write(enumerable);
        public static string ToTableString(this IEnumerable enumerable) => new TableString().Write(enumerable);

        public static void ToTableString<T>(this IEnumerable<T> enumerable, TextWriter writer) => new TableString().Write(writer, enumerable);
        public static void ToTableString(this IEnumerable enumerable, TextWriter writer) => new TableString().Write(writer, enumerable);
    }
}
