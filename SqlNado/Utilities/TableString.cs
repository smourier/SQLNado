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
        private List<TableStringColumn> _columns = new List<TableStringColumn>();

        public TableString()
        {
            IndentTabString = " ";
            //DefaultAlignment = ColumnStringAlignment.Left;
            //DefaultHeaderAlignment = DefaultAlignment;
            UseBuiltinStyle(TableStringStyle.BoxDrawingSingle);
            Padding = new TableStringPadding(1, 0);
            MaximumWidth = 140;
        }

        public virtual void AddColumn(TableStringColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            column.Index = _columns.Count;
            _columns.Add(column);
        }

        public string IndentTabString { get; set; }
        public int MaximumWidth { get; set; }
        public TableStringAlignment DefaultAlignment { get; set; }
        public TableStringAlignment DefaultHeaderAlignment { get; set; }
        public virtual IReadOnlyList<TableStringColumn> Columns => _columns;
        public virtual char TopLeftCharacter { get; set; }
        public virtual char TopMiddleCharacter { get; set; }
        public virtual char TopRightCharacter { get; set; }
        public virtual char BottomLeftCharacter { get; set; }
        public virtual char BottomMiddleCharacter { get; set; }
        public virtual char BottomRightCharacter { get; set; }
        public virtual char MiddleLeftCharacter { get; set; }
        public virtual char MiddleMiddleCharacter { get; set; }
        public virtual char MiddleRightCharacter { get; set; }
        public virtual char VerticalCharacter { get; set; }
        public virtual char HorizontalCharacter { get; set; }
        public virtual TableStringPadding Padding { get; set; }

        public virtual void UseUniformStyle(char c)
        {
            TopLeftCharacter = c;
            TopMiddleCharacter = c;
            TopRightCharacter = c;
            BottomLeftCharacter = c;
            BottomMiddleCharacter = c;
            BottomRightCharacter = c;
            MiddleLeftCharacter = c;
            MiddleMiddleCharacter = c;
            MiddleRightCharacter = c;
            VerticalCharacter = c;
            HorizontalCharacter = c;
        }

        public virtual void UseBuiltinStyle(TableStringStyle format)
        {
            switch (format)
            {
                case TableStringStyle.BoxDrawingDouble:
                    TopLeftCharacter = '╔';
                    TopMiddleCharacter = '╦';
                    TopRightCharacter = '╗';
                    BottomLeftCharacter = '╚';
                    BottomMiddleCharacter = '╩';
                    BottomRightCharacter = '╝';
                    MiddleLeftCharacter = '╠';
                    MiddleMiddleCharacter = '╬';
                    MiddleRightCharacter = '╣';
                    VerticalCharacter = '║';
                    HorizontalCharacter = '═';
                    break;

                case TableStringStyle.BoxDrawingSingle:
                    TopLeftCharacter = '┌';
                    TopMiddleCharacter = '┬';
                    TopRightCharacter = '┐';
                    BottomLeftCharacter = '└';
                    BottomMiddleCharacter = '┴';
                    BottomRightCharacter = '┘';
                    MiddleLeftCharacter = '├';
                    MiddleMiddleCharacter = '┼';
                    MiddleRightCharacter = '┤';
                    VerticalCharacter = '│';
                    HorizontalCharacter = '─';
                    break;

                default:
                    TopLeftCharacter = '+';
                    TopMiddleCharacter = '+';
                    TopRightCharacter = '+';
                    BottomLeftCharacter = '+';
                    BottomMiddleCharacter = '+';
                    BottomRightCharacter = '+';
                    MiddleLeftCharacter = '+';
                    MiddleMiddleCharacter = '+';
                    MiddleRightCharacter = '+';
                    VerticalCharacter = '|';
                    HorizontalCharacter = '-';
                    break;
            }
        }

        protected virtual void CreateColumns(object first)
        {
            if (first == null)
                throw new ArgumentNullException(nameof(first));

            if (first is Array array)
            {
                for (int i = 0; i < array.Length; i++)
                {
                    AddColumn(new ArrayItemTableColumnString(i));
                }
                return;
            }

            foreach (var property in first.GetType().GetProperties())
            {
                var browsable = property.GetCustomAttribute<BrowsableAttribute>();
                if (browsable != null && !browsable.Browsable)
                    continue;

                AddColumn(new PropertyInfoTableColumnString(property));
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

            // switch to indented writer if needed
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

            var rows = new List<TableStringCell[]>();
            int[] columnWidths = ComputeColumnWidths(enumerable, rows);
            if (columnWidths == null) // no valid columns
                return;

            // top line (only once) and others
            var bottomLine = new StringBuilder();
            var middleLine = new StringBuilder();
            var emptyLine = (Padding != null && Padding.HasVerticalPadding) ? new StringBuilder() : null;
            wr.Write(TopLeftCharacter);
            middleLine.Append(MiddleLeftCharacter);
            bottomLine.Append(BottomLeftCharacter);
            if (emptyLine != null)
            {
                emptyLine.Append(VerticalCharacter);
            }

            for (int i = 0; i < Columns.Count; i++)
            {
                if (i > 0)
                {
                    wr.Write(TopMiddleCharacter);
                    middleLine.Append(MiddleMiddleCharacter);
                    bottomLine.Append(BottomMiddleCharacter);
                }

                var bar = new string(HorizontalCharacter, columnWidths[i]);
                wr.Write(bar);
                middleLine.Append(bar);
                bottomLine.Append(bar);
                if (emptyLine != null)
                {
                    emptyLine.Append(new string(' ', columnWidths[i]));
                    emptyLine.Append(VerticalCharacter);
                }
            }
            wr.Write(TopRightCharacter);
            wr.WriteLine();
            middleLine.Append(MiddleRightCharacter);
            bottomLine.Append(BottomRightCharacter);

            if (Padding != null)
            {
                for (int l = 0; l < Padding.Top; l++)
                {
                    wr.WriteLine(emptyLine);
                }
            }

            string leftPadding = Padding != null ? new string(' ', Padding.Left) : null;
            string rightPadding = Padding != null ? new string(' ', Padding.Right) : null;

            wr.Write(VerticalCharacter);
            int hp = HorizontalPadding;
            for (int i = 0; i < Columns.Count; i++)
            {
                if (leftPadding != null)
                {
                    wr.Write(leftPadding);
                }
                var alignment = Columns[i].HeaderAlignment == TableStringAlignment.Unspecified ? DefaultHeaderAlignment : Columns[i].HeaderAlignment;
                string header = Align(Columns[i].Name, columnWidths[i] - hp, alignment);
                wr.Write(header);
                if (rightPadding != null)
                {
                    wr.Write(rightPadding);
                }
                wr.Write(VerticalCharacter);
            }
            wr.WriteLine();

            if (Padding != null)
            {
                for (int l = 0; l < Padding.Bottom; l++)
                {
                    wr.WriteLine(emptyLine);
                }
            }

            for (int rowIndex = 0; rowIndex < rows.Count; rowIndex++)
            {
                wr.WriteLine(middleLine);
                if (Padding != null)
                {
                    for (int l = 0; l < Padding.Top; l++)
                    {
                        wr.WriteLine(emptyLine);
                    }
                }

                var cells = rows[rowIndex];
                wr.Write(VerticalCharacter);
                for (int i = 0; i < Columns.Count; i++)
                {
                    if (leftPadding != null)
                    {
                        wr.Write(leftPadding);
                    }
                    var alignment = Columns[i].Alignment == TableStringAlignment.Unspecified ? DefaultAlignment : Columns[i].Alignment;
                    string value = Align(cells[i].Text, columnWidths[i] - hp, alignment);
                    wr.Write(value);
                    if (rightPadding != null)
                    {
                        wr.Write(rightPadding);
                    }
                    wr.Write(VerticalCharacter);
                }
                wr.WriteLine();

                if (Padding != null)
                {
                    for (int l = 0; l < Padding.Bottom; l++)
                    {
                        wr.WriteLine(emptyLine);
                    }
                }
            }

            wr.WriteLine(bottomLine.ToString());
        }

        private int HorizontalPadding => Padding != null ? Padding.Horizontal : 0;

        protected virtual int[] ComputeColumnWidths(IEnumerable enumerable, IList<TableStringCell[]> rows)
        {
            if (enumerable == null)
                throw new ArgumentNullException(nameof(enumerable));

            if (rows == null)
                throw new ArgumentNullException(nameof(rows));

            int[] desiredColumnWidths = null; // with h padding
            foreach (var row in enumerable)
            {
                if (Columns.Count == 0)
                {
                    // create the columns with the first non-null row that will create at least one column
                    if (row == null)
                        continue;

                    CreateColumns(row);
                    if (Columns.Count == 0)
                        continue;

                    desiredColumnWidths = new int[Columns.Count];
                }

                var cells = new TableStringCell[Columns.Count];
                for (int i = 0; i < Columns.Count; i++)
                {
                    //int maxLength = MaximumWidth > 0 ? int.MaxValue : 0;
                    var toCellFunc = Columns[i].ToCellFunc ?? TableStringContext.DefaultToCellFunc;
                    var ctx = CreateTableStringContext(Columns[i]);
                    ctx.Row = row;
                    cells[i] = toCellFunc(ctx);

                    //ComputeColumnWidth(i, row, desiredColumnWidths, cells, HorizontalPadding, maxLength);
                }

                rows.Add(cells);
            }

            if (MaximumWidth > 0)
            {
            }
            return desiredColumnWidths;
        }

        protected virtual TableStringContext CreateTableStringContext(TableStringColumn column) => new TableStringContext(column);

        private void ComputeColumnWidth(int columnIndex, object row, int[] desiredColumnWidths, TableStringCell[] cells, int horizontalPadding, int maxLength)
        {
            var toCellFunc = Columns[columnIndex].ToCellFunc ?? TableStringContext.DefaultToCellFunc;
            var ctx = CreateTableStringContext(Columns[columnIndex]);
            if (maxLength > 0)
            {
                ctx.MaxLength = maxLength;
            }
            ctx.Row = row;
            cells[columnIndex] = toCellFunc(ctx);

            int size = cells[columnIndex].DesiredColumnWith;
            if (size != int.MaxValue)
            {
                if (horizontalPadding > 0)
                {
                    size += horizontalPadding;
                }
            }

            if (size > desiredColumnWidths[columnIndex])
            {
                desiredColumnWidths[columnIndex] = size;
            }
        }

        public virtual string Align(string text, int maxLength, TableStringAlignment alignment)
        {
            string str;
            switch (alignment)
            {
                case TableStringAlignment.Left:
                    str = string.Format("{0,-" + maxLength + "}", text);
                    break;

                case TableStringAlignment.Center:
                    int spaces = maxLength - text.Length;
                    if (spaces == 0)
                    {
                        str = text;
                    }
                    else
                    {
                        int left = spaces - spaces / 2;
                        int right = spaces - left;
                        str = new string(' ', left) + text + new string(' ', right);
                    }
                    break;

                default:
                    str = string.Format("{0," + maxLength + "}", text);
                    break;
            }
            return str;
        }
    }

    public enum TableStringStyle
    {
        Ascii,
        BoxDrawingDouble,
        BoxDrawingSingle,
    }

    public class TableStringPadding
    {
        public TableStringPadding(int padding)
        {
            Left = padding;
            Right = padding;
            Top = padding;
            Bottom = padding;
        }

        public TableStringPadding(int horizontalPadding, int verticalPadding)
        {
            Left = horizontalPadding;
            Right = horizontalPadding;
            Top = verticalPadding;
            Bottom = verticalPadding;
        }

        public TableStringPadding(int left, int top, int right, int bottom)
        {
            Left = left;
            Top = top;
            Right = right;
            Bottom = bottom;
        }

        public int Left { get; set; }
        public int Right { get; set; }
        public int Top { get; set; }
        public int Bottom { get; set; }

        public int Horizontal => Left + Right;
        public int Vertival => Top + Bottom;
        public bool HasVerticalPadding => Top > 0 || Bottom > 0;
        public bool HasHorizontalPadding => Left > 0 || Right > 0;
    }

    public class TableStringContext
    {
        public const int MaxDefault = 50;

        public static readonly Func<TableStringContext, TableStringCell> DefaultToCellFunc = (c) =>
        {
            var cell = new TableStringCell();
            cell.Value = c.GetValue();
            if (!(cell.Value is string s))
            {
                s = string.Format(c.FormatProvider, "{0}", cell.Value);
            }
            cell.DesiredColumnWith = s.Length;
            return cell;
        };

        //public static readonly Func<TableStringContext, string> DefaultToStringFunc = (c) =>
        //{
        //    var value = c.GetValue();
        //    if (value is string s)
        //        return c.FinalizeString(s);

        //    if (value is byte[] bytes)
        //    {
        //        int max = c.MaxLength;
        //        if (max <= 0)
        //        {
        //            max = MaxDefault;
        //        }

        //        if (bytes.Length > (max - 1) / 2)
        //            return "0x" + BitConverter.ToString(bytes, 0, (max - 1) / 2).Replace("-", string.Empty) + c.Hyphens + " (" + bytes.Length + ")";

        //        return "0x" + BitConverter.ToString(bytes, 0, Math.Min((max - 1) / 2, bytes.Length)).Replace("-", string.Empty);
        //    }

        //    return c.FinalizeString(string.Format("{0}", value));
        //};

        private int? _maxLength;

        public TableStringContext(TableStringColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            Column = column;
            NewLineReplacement = "\u001A";
            Hyphens = "...";
        }

        public TableStringColumn Column { get; }
        public IFormatProvider FormatProvider { get; set; }
        public virtual object Row { get; set; }
        public string NewLineReplacement { get; set; }
        public string Hyphens { get; set; }

        public int MaxLength
        {
            get
            {
                return _maxLength.HasValue ? _maxLength.Value : Column.MaxLength;
            }
            set
            {
                _maxLength = value;
            }
        }

        public virtual object GetValue() => Column.GetValueFunc(this);

        public virtual string FinalizeString(string str)
        {
            if (str == null)
                return str;

            int max = MaxLength;
            if (max <= 0)
            {
                max = MaxDefault;
            }

            if (NewLineReplacement != null) // can be empty string
            {
                str = str.Replace(Environment.NewLine, NewLineReplacement);
            }

            if (str.Length < max)
                return str;

            if (max < Hyphens.Length)
                return str.Substring(0, max);

            return str.Substring(0, max - Hyphens.Length) + Hyphens;
        }
    }

    public enum TableStringAlignment
    {
        Unspecified,
        Right,
        Left,
        Center,
    }

    public class TableStringColumn
    {
        public TableStringColumn(string name, Func<TableStringContext, object> getValueFunc)
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
        public TableStringAlignment Alignment { get; set; }
        public TableStringAlignment HeaderAlignment { get; set; }
        public Func<TableStringContext, TableStringCell> ToCellFunc { get; set; }
    }

    public class TableStringCell
    {
        public object Value { get; set; }
        public int DesiredColumnWith { get; set; }

        public string Text { get; private set; }

        public virtual void ComputeText(TableStringContext context)
        {
            if (context == null)
                throw new ArgumentNullException(nameof(context));
        }
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
            AddColumn(new TableStringColumn("Name", (c) => ((Tuple<string, object>)c.Row).Item1) { HeaderAlignment = TableStringAlignment.Left, Alignment = TableStringAlignment.Left });
            AddColumn(new TableStringColumn("Value", (c) => ((Tuple<string, object>)c.Row).Item2));
        }

        public void Write(int indent, TextWriter writer) => Write(indent, writer, Values);
    }

    public class ArrayItemTableColumnString : TableStringColumn
    {
        public ArrayItemTableColumnString(int index)
            : base("#" + index.ToString(), (c) => ((Array)c.Row).GetValue(((ArrayItemTableColumnString)c.Column).ArrayIndex))
        {
            ArrayIndex = index;
        }

        public int ArrayIndex { get; } // could be different from column's index
    }

    public class PropertyInfoTableColumnString : TableStringColumn
    {
        // yes, performance could be inproved (delegates, etc.)
        public PropertyInfoTableColumnString(PropertyInfo property)
            : base(property?.Name, (c) => ((PropertyInfoTableColumnString)c.Column).Property.GetValue(c.Row))
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
