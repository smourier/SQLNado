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
        private int _minimumColumnWidth;
        private int _indent;
        private int _defaultCellMaxLength;

        public TableString()
        {
            MinimumColumnWidth = 1;
            CanReduceCellPadding = true;
            IndentTabString = " ";
            UseBuiltinStyle(TableStringStyle.BoxDrawingSingle);
            CellPadding = new TableStringPadding(1, 0);
            MaximumWidth = 140;

            DefaultCellAlignment = TableStringAlignment.Right;
            DefaultHeaderCellAlignment = DefaultCellAlignment;
            DefaultNewLineReplacement = "\u001A";
            DefaultHyphens = "...";
            DefaultCellMaxLength = int.MaxValue;
            DefaultFormatProvider = null; // current culture
        }

        public virtual void AddColumn(TableStringColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            column.Index = _columns.Count;
            _columns.Add(column);
        }

        public int Indent { get => _indent; set => _indent = Math.Min(value, MaximumWidth - (MinimumColumnWidth + 2)); }
        public string IndentTabString { get; set; }
        public int MaximumWidth { get; set; }
        public int MaximumRowHeight { get; set; } // if Wrap = true
        public int MinimumColumnWidth { get => _minimumColumnWidth; set => _minimumColumnWidth = Math.Max(value, 1); }
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
        public virtual TableStringPadding CellPadding { get; set; }
        public virtual bool CanReduceCellPadding { get; set; }
        public virtual bool CellWrap { get; set; } // only if MaximumWidth

        // default column settings
        public TableStringAlignment DefaultCellAlignment { get; set; }
        public TableStringAlignment DefaultHeaderCellAlignment { get; set; }
        public virtual string DefaultNewLineReplacement { get; set; }
        public virtual string DefaultHyphens { get; set; }
        public virtual int DefaultCellMaxLength { get => _defaultCellMaxLength; set => _defaultCellMaxLength = Math.Max(value, 1); }
        public virtual IFormatProvider DefaultFormatProvider { get; set; }

        public int MaximumNumberOfColumnsWithoutPadding
        {
            get
            {
                if (MaximumWidth <= 0)
                    return int.MaxValue;

                return (MaximumWidth - 1) / (1 + MinimumColumnWidth);
            }
        }

        public int MaximumNumberOfColumns
        {
            get
            {
                if (MaximumWidth <= 0)
                    return int.MaxValue;

                if (CellPadding == null)
                    return MaximumNumberOfColumnsWithoutPadding;

                return (MaximumWidth - 1) / (1 + MinimumColumnWidth + CellPadding.Horizontal);
            }
        }

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
                    AddColumn(new ArrayItemTableColumnString(this, i));
                }
                return;
            }

            foreach (var property in first.GetType().GetProperties())
            {
                var browsable = property.GetCustomAttribute<BrowsableAttribute>();
                if (browsable != null && !browsable.Browsable)
                    continue;

                AddColumn(new PropertyInfoTableColumnString(this, property));
            }
        }

        public virtual string Write(IEnumerable enumerable)
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

            _columns.Clear();

            // something to write?
            if (enumerable == null)
                return;

            // switch to indented writer if needed
            TextWriter wr;
            if (Indent > 0)
            {
                var itw = new IndentedTextWriter(writer, IndentTabString);
                itw.Indent = Indent;
                for (int i = 0; i < Indent; i++)
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
            var header = new List<TableStringCell>();
            int columnsCount = ComputeColumnWidths(enumerable, header, rows);
            if (columnsCount == 0) // no valid columns
                return;

            // top line (only once) and others
            var bottomLine = new StringBuilder();
            var middleLine = new StringBuilder();
            var emptyLine = (CellPadding != null && CellPadding.HasVerticalPadding) ? new StringBuilder() : null;
            wr.Write(TopLeftCharacter);
            middleLine.Append(MiddleLeftCharacter);
            bottomLine.Append(BottomLeftCharacter);
            if (emptyLine != null)
            {
                emptyLine.Append(VerticalCharacter);
            }

            for (int i = 0; i < columnsCount; i++)
            {
                if (i > 0)
                {
                    wr.Write(TopMiddleCharacter);
                    middleLine.Append(MiddleMiddleCharacter);
                    bottomLine.Append(BottomMiddleCharacter);
                }

                var bar = new string(HorizontalCharacter, Columns[i].Width);
                wr.Write(bar);
                middleLine.Append(bar);
                bottomLine.Append(bar);
                if (emptyLine != null)
                {
                    emptyLine.Append(new string(' ', Columns[i].Width));
                    emptyLine.Append(VerticalCharacter);
                }
            }
            wr.Write(TopRightCharacter);
            wr.WriteLine();
            middleLine.Append(MiddleRightCharacter);
            bottomLine.Append(BottomRightCharacter);

            if (CellPadding != null)
            {
                for (int l = 0; l < CellPadding.Top; l++)
                {
                    wr.WriteLine(emptyLine);
                }
            }

            string leftPadding = CellPadding != null ? new string(' ', CellPadding.Left) : null;
            string rightPadding = CellPadding != null ? new string(' ', CellPadding.Right) : null;

            wr.Write(VerticalCharacter);
            int hp = HorizontalPadding;
            for (int i = 0; i < columnsCount; i++)
            {
                if (leftPadding != null)
                {
                    wr.Write(leftPadding);
                }

                string headerCell = Columns[i].ComputeDisplayStrings(header[i])[0];
                wr.Write(headerCell);
                if (rightPadding != null)
                {
                    wr.Write(rightPadding);
                }
                wr.Write(VerticalCharacter);
            }
            wr.WriteLine();

            if (CellPadding != null)
            {
                for (int l = 0; l < CellPadding.Bottom; l++)
                {
                    wr.WriteLine(emptyLine);
                }
            }

            for (int rowIndex = 0; rowIndex < rows.Count; rowIndex++)
            {
                wr.WriteLine(middleLine);
                if (CellPadding != null)
                {
                    for (int l = 0; l < CellPadding.Top; l++)
                    {
                        wr.WriteLine(emptyLine);
                    }
                }

                var cells = rows[rowIndex];
                wr.Write(VerticalCharacter);
                for (int i = 0; i < columnsCount; i++)
                {
                    if (leftPadding != null)
                    {
                        wr.Write(leftPadding);
                    }

                    string value = Columns[i].ComputeDisplayStrings(cells[i])[0];
                    wr.Write(value);
                    if (rightPadding != null)
                    {
                        wr.Write(rightPadding);
                    }
                    wr.Write(VerticalCharacter);
                }
                wr.WriteLine();

                if (CellPadding != null)
                {
                    for (int l = 0; l < CellPadding.Bottom; l++)
                    {
                        wr.WriteLine(emptyLine);
                    }
                }
            }

            wr.WriteLine(bottomLine.ToString());
        }

        private int HorizontalPadding => CellPadding != null ? CellPadding.Horizontal : 0;

        protected virtual int ComputeColumnWidths(IEnumerable enumerable, IList<TableStringCell> header, IList<TableStringCell[]> rows)
        {
            if (enumerable == null)
                throw new ArgumentNullException(nameof(enumerable));

            if (header == null)
                throw new ArgumentNullException(nameof(header));

            if (rows == null)
                throw new ArgumentNullException(nameof(rows));

            header.Clear();
            rows.Clear();
            int[] desiredPaddedColumnWidths = null; // with h padding
            var hp = HorizontalPadding;
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

                    desiredPaddedColumnWidths = new int[Math.Min(Columns.Count, MaximumNumberOfColumns)];

                    // compute header rows
                    for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                    {
                        var cell = CreateCell(Columns[i], Columns[i]);
                        header.Add(cell);
                        cell.ComputeText();

                        int size = cell.DesiredColumnWith;
                        if (size != int.MaxValue)
                        {
                            if (hp > 0)
                            {
                                size += hp;
                            }
                        }

                        if (size > desiredPaddedColumnWidths[i])
                        {
                            desiredPaddedColumnWidths[i] = size;
                        }
                    }
                }

                var cells = new TableStringCell[desiredPaddedColumnWidths.Length];
                for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                {
                    object value = Columns[i].GetValueFunc(Columns[i], row);
                    cells[i] = CreateCell(Columns[i], value);
                    cells[i].ComputeText();

                    int size = cells[i].DesiredColumnWith;
                    if (size != int.MaxValue)
                    {
                        if (hp > 0)
                        {
                            size += hp;
                        }
                    }

                    if (size > desiredPaddedColumnWidths[i])
                    {
                        desiredPaddedColumnWidths[i] = size;
                    }
                }

                rows.Add(cells);
            }

            if (MaximumWidth <= 0)
            {
                for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                {
                    Columns[i].Width = desiredPaddedColumnWidths[i];
                }
            }
            else
            {
                int maxWidth = MaximumWidth - Indent;
                var unpadded = new bool[desiredPaddedColumnWidths.Length];
                int desiredWidth = desiredPaddedColumnWidths.Sum();
                if (desiredWidth > maxWidth)
                {
                    if (CanReduceCellPadding)
                    {
                        int diff = desiredWidth - maxWidth;
                        int paddingSize = desiredPaddedColumnWidths.Length * hp;

                        // remove padding from last column to first
                        for (int i = desiredPaddedColumnWidths.Length - 1; i >= 0; i--)
                        {
                            unpadded[i] = true;
                            diff -= hp;
                            if (diff <= 0)
                                break;
                        }
                    }

                    int availableWidth = maxWidth;
                    do
                    {
                        var uncomputedColumns = Columns.Take(desiredPaddedColumnWidths.Length).Where(c => c.Width < 0).ToArray();
                        if (uncomputedColumns.Length == 0)
                            break;

                        int avgWidth = availableWidth / uncomputedColumns.Length;
                        int computed = 0;
                        foreach (var column in uncomputedColumns)
                        {
                            if (desiredPaddedColumnWidths[column.Index] <= avgWidth)
                            {
                                column.Width = desiredPaddedColumnWidths[column.Index];
                                if (unpadded[column.Index])
                                {
                                    column.Width -= hp;
                                }
                                availableWidth -= column.Width;
                                computed++;
                            }
                        }

                        if (computed == 0)
                        {
                            avgWidth = availableWidth / uncomputedColumns.Length;
                            foreach (var column in uncomputedColumns)
                            {
                                column.Width = avgWidth;
                                if (!unpadded[column.Index])
                                {
                                    column.Width += hp;
                                }
                                availableWidth -= column.Width;
                            }
                        }
                    }
                    while (true);

                    // adjust line column, because of roundings
                    int totalWidth = Columns.Take(desiredPaddedColumnWidths.Length).Sum(c => c.Width);
                    if (totalWidth < maxWidth)
                    {
                        Columns[desiredPaddedColumnWidths.Length - 1].Width += maxWidth - totalWidth;
                    }
                }
                else
                {
                    for (int i = 0; i < desiredPaddedColumnWidths.Length; i++)
                    {
                        Columns[i].Width = desiredPaddedColumnWidths[i];
                    }
                }
            }
            return desiredPaddedColumnWidths.Length;
        }

        protected virtual TableStringColumn CreateColumn(string name, Func<TableStringColumn, object, object> getValueFunc) => new TableStringColumn(this, name, getValueFunc);

        protected virtual TableStringCell CreateCell(TableStringColumn column, object value)
        {
            if (value != null && value.Equals(column))
                return new HeaderTableStringCell(column);

            if (value is byte[] bytes)
                return new TableStringCell(column, bytes);

            return new TableStringCell(column, value);
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
        private int _left;
        private int _right;
        private int _top;
        private int _bottom;

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

        public int Left { get => _left; set => _left = Math.Max(0, value); }
        public int Right { get => _right; set => _right = Math.Max(0, value); }
        public int Top { get => _top; set => _top = Math.Max(0, value); }
        public int Bottom { get => _bottom; set => _bottom = Math.Max(0, value); }

        public int Horizontal => Left + Right;
        public int Vertival => Top + Bottom;
        public bool HasVerticalPadding => Top > 0 || Bottom > 0;
        public bool HasHorizontalPadding => Left > 0 || Right > 0;
    }

    //public class TableStringContext
    //{
    //    public const int MaxDefault = 50;

    //    public static readonly Func<TableStringContext, TableStringCell> DefaultToCellFunc = (c) =>
    //    {
    //        var cell = new TableStringCell();
    //        cell.Value = c.GetValue();
    //        if (!(cell.Value is string s))
    //        {
    //            s = string.Format(c.FormatProvider, "{0}", cell.Value);
    //        }
    //        cell.DesiredColumnWith = s.Length;
    //        return cell;
    //    };

    //    //public static readonly Func<TableStringContext, string> DefaultToStringFunc = (c) =>
    //    //{
    //    //    var value = c.GetValue();
    //    //    if (value is string s)
    //    //        return c.FinalizeString(s);

    //    //    if (value is byte[] bytes)
    //    //    {
    //    //        int max = c.MaxLength;
    //    //        if (max <= 0)
    //    //        {
    //    //            max = MaxDefault;
    //    //        }

    //    //        if (bytes.Length > (max - 1) / 2)
    //    //            return "0x" + BitConverter.ToString(bytes, 0, (max - 1) / 2).Replace("-", string.Empty) + c.Hyphens + " (" + bytes.Length + ")";

    //    //        return "0x" + BitConverter.ToString(bytes, 0, Math.Min((max - 1) / 2, bytes.Length)).Replace("-", string.Empty);
    //    //    }

    //    //    return c.FinalizeString(string.Format("{0}", value));
    //    //};

    //    private int? _maxLength;

    //    public TableStringContext(TableStringColumn column)
    //    {
    //        if (column == null)
    //            throw new ArgumentNullException(nameof(column));

    //        Column = column;
    //        NewLineReplacement = "\u001A";
    //        Hyphens = "...";
    //    }

    //    public TableStringColumn Column { get; }
    //    public IFormatProvider FormatProvider { get; set; }
    //    public virtual object Row { get; set; }
    //    public string NewLineReplacement { get; set; }
    //    public string Hyphens { get; set; }

    //    public int MaxLength
    //    {
    //        get
    //        {
    //            return _maxLength.HasValue ? _maxLength.Value : Column.MaxLength;
    //        }
    //        set
    //        {
    //            _maxLength = value;
    //        }
    //    }

    //    public virtual object GetValue() => Column.GetValueFunc(this);

    //    public virtual string ComputeDisplay(string str)
    //    {
    //        if (str == null)
    //            return str;

    //        int max = MaxLength;
    //        if (max <= 0)
    //        {
    //            max = MaxDefault;
    //        }

    //        if (NewLineReplacement != null) // can be empty string
    //        {
    //            str = str.Replace(Environment.NewLine, NewLineReplacement);
    //        }

    //        if (str.Length < max)
    //            return str;

    //        if (max < Hyphens.Length)
    //            return str.Substring(0, max);

    //        return str.Substring(0, max - Hyphens.Length) + Hyphens;
    //    }
    //}

    public enum TableStringAlignment
    {
        Right,
        Left,
        Center,
    }

    public class TableStringColumn
    {
        private int? _maxLength;
        private IFormatProvider _formatProvider;
        private TableStringAlignment? _aligment;
        private TableStringAlignment? _headerAligment;
        private string _hyphens;
        private int _width = -1;
        private string _newLineReplacement;

        public TableStringColumn(TableString table, string name, Func<TableStringColumn, object, object> getValueFunc)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            if (name == null)
                throw new ArgumentNullException(nameof(name));

            if (getValueFunc == null)
                throw new ArgumentNullException(nameof(getValueFunc));

            Table = table;
            GetValueFunc = getValueFunc;
            Name = name;
            Width = -1;
        }

        public TableString Table { get; }
        public string Name { get; }
        public Func<TableStringColumn, object, object> GetValueFunc { get; }
        public int Index { get; internal set; }
        public int Width { get => _width; internal set => _width = Math.Min(MaxLength, value); }

        public virtual int MaxLength { get => _maxLength ?? Table.DefaultCellMaxLength; set => _maxLength = value; }
        public virtual string Hyphens { get => _hyphens ?? Table.DefaultHyphens; set => _hyphens = value; }
        public virtual string NewLineReplacement { get => _newLineReplacement ?? Table.DefaultNewLineReplacement; set => _newLineReplacement = value; }
        public virtual IFormatProvider FormatProvider { get => _formatProvider ?? Table.DefaultFormatProvider; set => _formatProvider = value; }
        public virtual TableStringAlignment Alignment { get => _aligment ?? Table.DefaultCellAlignment; set => _aligment = value; }
        public virtual TableStringAlignment HeaderAlignment { get => _headerAligment ?? Table.DefaultHeaderCellAlignment; set => _headerAligment = value; }

        public override string ToString() => Name;

        public virtual string[] ComputeDisplayStrings(TableStringCell cell)
        {
            if (cell == null)
                throw new ArgumentNullException(nameof(cell));

            if (cell.Text == null)
                return new string[] { null };

            if (cell.Text.Length > Width)
                return new string[] { cell.Text.Substring(0, Width) };

            return new string[] { cell.Text };
        }

        //public virtual string Align(string text, int maxLength, TableStringAlignment alignment)
        //{
        //    string str;
        //    switch (alignment)
        //    {
        //        case TableStringAlignment.Left:
        //            str = string.Format("{0,-" + maxLength + "}", text);
        //            break;

        //        case TableStringAlignment.Center:
        //            int spaces = maxLength - (text != null ? text.Length : 0);
        //            if (spaces == 0)
        //            {
        //                str = text;
        //            }
        //            else
        //            {
        //                int left = spaces - spaces / 2;
        //                int right = spaces - left;
        //                str = new string(' ', left) + text + new string(' ', right);
        //            }
        //            break;

        //        default:
        //            str = string.Format("{0," + maxLength + "}", text);
        //            break;
        //    }
        //    return str;
        //}
    }

    public class TableStringCell
    {
        public TableStringCell(TableStringColumn column, object value)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            Column = column;
            Value = value;
        }

        public TableStringColumn Column { get; }
        public object Value { get; }
        public virtual string Text { get; protected set; }

        public virtual int DesiredColumnWith
        {
            get
            {
                if (Text == null)
                    return 0;

                if (Column.MaxLength <= 0)
                    return Text.Length;

                return Math.Min(Text.Length, Column.MaxLength);
            }
        }

        public override string ToString() => Text;

        public virtual void ComputeText()
        {
            if (!(Value is string s))
            {
                s = string.Format(Column.FormatProvider, "{0}", Value);
            }
            Text = s;
        }
    }

    public class HeaderTableStringCell : TableStringCell
    {
        public HeaderTableStringCell(TableStringColumn column)
            : base(column, column?.Name)
        {
        }
    }

    public class ByteArrayTableStringCell : TableStringCell
    {
        public ByteArrayTableStringCell(TableStringColumn column, byte[] bytes)
            : base(column, bytes)
        {
        }

        public new byte[] Value => (byte[])base.Value;

        public override void ComputeText()
        {
            var bytes = Value;
            int max = Column.MaxLength;
            if (max <= 0)
            {
                max = 50;
            }

            if (bytes.Length > (max - 1) / 2)
            {
                Text = "0x" + BitConverter.ToString(bytes, 0, (max - 1) / 2).Replace("-", string.Empty) + Column.Hyphens + " (" + bytes.Length + ")";
            }

            Text = "0x" + BitConverter.ToString(bytes, 0, Math.Min((max - 1) / 2, bytes.Length)).Replace("-", string.Empty);
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
            var nameColumn = CreateColumn("Name", (c, r) => ((Tuple<string, object>)r).Item1);
            nameColumn.HeaderAlignment = TableStringAlignment.Left;
            nameColumn.Alignment = nameColumn.HeaderAlignment;
            AddColumn(nameColumn);
            AddColumn(CreateColumn("Value", (c, r) => ((Tuple<string, object>)r).Item2));
        }

        public void WriteObject(TextWriter writer) => Write(writer, Values);
        public virtual string WriteObject()
        {
            using (var sw = new StringWriter())
            {
                WriteObject(sw);
                return sw.ToString();
            }
        }
    }

    public class ArrayItemTableColumnString : TableStringColumn
    {
        public ArrayItemTableColumnString(TableString table, int index)
            : base(table, "#" + index.ToString(), (c, r) => ((Array)r).GetValue(((ArrayItemTableColumnString)c).ArrayIndex))
        {
            ArrayIndex = index;
        }

        public int ArrayIndex { get; } // could be different from column's index
    }

    public class PropertyInfoTableColumnString : TableStringColumn
    {
        // yes, performance could be inproved (delegates, etc.)
        public PropertyInfoTableColumnString(TableString table, PropertyInfo property)
            : base(table, property?.Name, (c, r) => ((PropertyInfoTableColumnString)c).Property.GetValue(r))
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

        public static string ToTableString(int indent, object obj) => new ObjectTableString(obj) { Indent = indent }.WriteObject();
        public static string ToTableString(object obj) => new ObjectTableString(obj).WriteObject();
        public static void ToTableString(int indent, object obj, TextWriter writer) => new ObjectTableString(obj) { Indent = indent }.WriteObject(writer);
        public static void ToTableString(object obj, TextWriter writer) => new ObjectTableString(obj).WriteObject(writer);
    }
}
