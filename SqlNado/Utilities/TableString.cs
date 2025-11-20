namespace SqlNado.Utilities;

public class TableString
{
    private static readonly Lazy<bool> _isRunningInKudu = new(() => Environment.GetEnvironmentVariables().Cast<DictionaryEntry>().Any(e => ((string)e.Key).IndexOf("kudu", StringComparison.OrdinalIgnoreCase) >= 0), true);
    public static bool IsRunningInKudu => _isRunningInKudu.Value;

    private const int _columnBorderWidth = 1;
    private const int _absoluteMinimumColumnWidth = 1;
    private static readonly Lazy<bool> _isConsoleValid = new(GetConsoleValidity, true);
    private static int _defaultMaximumWidth = ConsoleWindowWidth;
    private readonly List<TableStringColumn> _columns = [];
    private int _minimumColumnWidth;
    private int _maximumWidth;
    private int _maximumRowHeight;
    private int _maximumByteArrayDisplayCount;
    private int _indent;
    private int _defaultCellMaxLength;
    private char _defaultNewLineReplacement;
    private char _defaultNonPrintableReplacement;
    private string? _defaultHyphens;
    private ConsoleColor? _defaultHeaderForegroundColor;
    private ConsoleColor? _defaultHeaderBackgroundColor;
    private ConsoleColor? _defaultForegroundColor;
    private ConsoleColor? _defaultBackgroundColor;

    public TableString()
    {
        MinimumColumnWidth = 1;
        CanReduceCellPadding = true;
        IndentTabString = " ";
        TabString = "    ";
        UseBuiltinStyle(IsRunningInKudu ? TableStringStyle.Ascii : TableStringStyle.BoxDrawingSingle);
        CellPadding = new TableStringPadding(1, 0);
        MaximumWidth = GlobalMaximumWidth;
        MaximumRowHeight = 50;
        MaximumByteArrayDisplayCount = 64;
        CellWrap = true;
        ThrowOnPropertyGetError = true;

        DefaultCellAlignment = TableStringAlignment.Left;
        DefaultHeaderCellAlignment = DefaultCellAlignment;
        DefaultNewLineReplacement = '\u001A';
        DefaultNonPrintableReplacement = '.';
        DefaultHyphens = "...";
        DefaultCellMaxLength = int.MaxValue;
        DefaultFormatProvider = null; // current culture
        GlobalHeaderForegroundColor = ConsoleColor.White;
    }

    public virtual void AddColumn(TableStringColumn column)
    {
        if (column == null)
            throw new ArgumentNullException(nameof(column));

        column.Index = _columns.Count;
        _columns.Add(column);
    }

    public int Indent { get => _indent; set => _indent = Math.Max(0, Math.Min(value, MaximumWidth - (MinimumColumnWidth + 2 * _columnBorderWidth))); }
    public string IndentTabString { get; set; }
    public string TabString { get; set; }
    public int MaximumWidth { get => _maximumWidth; set => _maximumWidth = Math.Max(value, _absoluteMinimumColumnWidth + 2 * _columnBorderWidth); }
    public int MaximumRowHeight { get => _maximumRowHeight; set => _maximumRowHeight = Math.Max(value, 1); }
    public int MinimumColumnWidth { get => _minimumColumnWidth; set => _minimumColumnWidth = Math.Max(value, _absoluteMinimumColumnWidth); }
    public int MaximumByteArrayDisplayCount { get => _maximumByteArrayDisplayCount; set => _maximumByteArrayDisplayCount = Math.Max(value, 0); }
    public virtual IReadOnlyList<TableStringColumn> Columns => _columns;
    public virtual bool ThrowOnPropertyGetError { get; set; }
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
    public virtual TableStringPadding? CellPadding { get; set; }
    public virtual bool CanReduceCellPadding { get; set; }
    public virtual bool CellWrap { get; set; }
    public virtual Func<char, char>? PrintCharFunc { get; set; }

    // default column settings
    public TableStringAlignment DefaultCellAlignment { get; set; }
    public TableStringAlignment DefaultHeaderCellAlignment { get; set; }
    public virtual char DefaultNewLineReplacement { get => _defaultNewLineReplacement; set => _defaultNewLineReplacement = value; }
    public virtual char DefaultNonPrintableReplacement { get => _defaultNonPrintableReplacement; set => _defaultNonPrintableReplacement = ToPrintable(value); }
    public virtual string? DefaultHyphens { get => _defaultHyphens; set => _defaultHyphens = value ?? string.Empty; }
    public virtual int DefaultCellMaxLength { get => _defaultCellMaxLength; set => _defaultCellMaxLength = Math.Max(value, 1); }
    public virtual IFormatProvider? DefaultFormatProvider { get; set; }
    public virtual ConsoleColor? DefaultHeaderForegroundColor { get => _defaultHeaderForegroundColor ?? GlobalHeaderForegroundColor; set => _defaultHeaderForegroundColor = value; }
    public virtual ConsoleColor? DefaultHeaderBackgroundColor { get => _defaultHeaderBackgroundColor ?? GlobalHeaderBackgroundColor; set => _defaultHeaderBackgroundColor = value; }
    public virtual ConsoleColor? DefaultForegroundColor { get => _defaultForegroundColor ?? GlobalForegroundColor; set => _defaultForegroundColor = value; }
    public virtual ConsoleColor? DefaultBackgroundColor { get => _defaultBackgroundColor ?? GlobalBackgroundColor; set => _defaultBackgroundColor = value; }

    public static int GlobalMaximumWidth { get => _defaultMaximumWidth; set => _defaultMaximumWidth = Math.Max(value, _absoluteMinimumColumnWidth); }
    public static int ConsoleMaximumNumberOfColumns => new TableString { MaximumWidth = ConsoleWindowWidth }.MaximumNumberOfColumnsWithoutPadding;
    public static ConsoleColor? GlobalHeaderForegroundColor { get; set; }
    public static ConsoleColor? GlobalHeaderBackgroundColor { get; set; }
    public static ConsoleColor? GlobalForegroundColor { get; set; }
    public static ConsoleColor? GlobalBackgroundColor { get; set; }
    public static bool IsConsoleValid => _isConsoleValid.Value;
    public static int ConsoleWindowWidth => IsConsoleValid ? Console.WindowWidth : int.MaxValue;

    private static bool GetConsoleValidity()
    {
        try
        {
            _ = Console.WindowWidth;
            return true;
        }
        catch
        {
            return false;
        }
    }

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

    // we need this because the console textwriter does WriteLine by its own...
    private sealed class ConsoleModeTextWriter(TextWriter writer, int maximumWidth) : TextWriter
    {
        private readonly int _maximumWidth = maximumWidth;
        private int _column;
        private bool _lastWasNewLine;

        public int Line { get; set; }
        public TextWriter Writer { get; } = writer;

        public override Encoding Encoding => Writer.Encoding;

        public override void WriteLine()
        {
            if (_lastWasNewLine)
            {
                _lastWasNewLine = false;
                return;
            }
            Writer.WriteLine();
            _column = 0;
            Line++;
        }

        public override void WriteLine(string? value)
        {
            Write(value);
            WriteLine();
        }

        public override void Write(char value)
        {
            Writer.Write(value);
            _column++;
            if (_column == _maximumWidth)
            {
                _lastWasNewLine = true;
                Line++;
                _column = 0;
            }
            else
            {
                _lastWasNewLine = false;
            }
        }

        public override void Write(string? value)
        {
            if (value == null)
                return;
#if DEBUG
            if (value.IndexOf(Environment.NewLine) >= 0)
                throw new NotSupportedException();
#endif
            Writer.Write(value);
            _column += value.Length;
            if (_column == _maximumWidth)
            {
                _lastWasNewLine = true;
                Line++;
                _column = 0;
            }
            else
            {
                _lastWasNewLine = false;
            }
#if DEBUG
            if (_column > _maximumWidth)
                throw new InvalidOperationException();
#endif
        }
    }

    public virtual string Write(IEnumerable? enumerable)
    {
        using var sw = new StringWriter();
        Write(sw, enumerable);
        return sw.ToString();
    }

    public virtual void Write(TextWriter writer, IEnumerable? enumerable)
    {
        if (writer == null)
            throw new ArgumentNullException(nameof(writer));

        _columns.Clear();

        // something to write?
        if (enumerable == null)
            return;

        var consoleMode = IsInConsoleMode(writer);
        var useConsoleWriter = MaximumWidth > 0 && consoleMode && ConsoleWindowWidth == MaximumWidth;
        var cw = useConsoleWriter ? new ConsoleModeTextWriter(writer, MaximumWidth) : writer;

        // switch to indented writer if needed
        TextWriter wr;
        if (Indent > 0)
        {
            var itw = new IndentedTextWriter(cw, IndentTabString)
            {
                Indent = Indent
            };
            for (var i = 0; i < Indent; i++)
            {
                cw.Write(IndentTabString);
            }
            wr = itw;
        }
        else
        {
            wr = cw;
        }

        var rows = new List<TableStringCell[]>();
        var headerCells = new List<TableStringCell>();
        var columnsCount = ComputeColumnWidths(writer, enumerable, headerCells, rows);
        if (columnsCount == 0) // no valid columns
            return;

        // top line (only once) and others
        var bottomLine = new StringBuilder();
        var middleLine = new StringBuilder();
        var emptyLine = (CellPadding != null && CellPadding.HasVerticalPadding) ? new StringBuilder() : null;
        wr.Write(TopLeftCharacter);
        middleLine.Append(MiddleLeftCharacter);
        bottomLine.Append(BottomLeftCharacter);
        emptyLine?.Append(VerticalCharacter);

        for (var i = 0; i < columnsCount; i++)
        {
            if (i > 0)
            {
                wr.Write(TopMiddleCharacter);
                middleLine.Append(MiddleMiddleCharacter);
                bottomLine.Append(BottomMiddleCharacter);
            }

            var bar = new string(HorizontalCharacter, Columns[i].WidthWithPadding);
            wr.Write(bar);
            middleLine.Append(bar);
            bottomLine.Append(bar);
            if (emptyLine != null)
            {
                emptyLine.Append(new string(' ', Columns[i].WidthWithPadding));
                emptyLine.Append(VerticalCharacter);
            }
        }
        wr.Write(TopRightCharacter);
        wr.WriteLine();
        middleLine.Append(MiddleRightCharacter);
        bottomLine.Append(BottomRightCharacter);

        if (CellPadding != null)
        {
            for (var l = 0; l < CellPadding.Top; l++)
            {
                wr.WriteLine(emptyLine);
            }
        }

        var leftPadding = CellPadding != null ? new string(' ', CellPadding.Left) : null;
        var rightPadding = CellPadding != null ? new string(' ', CellPadding.Right) : null;

        wr.Write(VerticalCharacter);
        for (var i = 0; i < columnsCount; i++)
        {
            if (leftPadding != null && Columns[i].IsHorizontallyPadded)
            {
                wr.Write(leftPadding);
            }

            headerCells[i].ComputeTextLines();
            headerCells[i].WriteTextLine(wr, 0);

            if (rightPadding != null && Columns[i].IsHorizontallyPadded)
            {
                wr.Write(rightPadding);
            }
            wr.Write(VerticalCharacter);
        }
        wr.WriteLine();

        if (CellPadding != null)
        {
            for (var l = 0; l < CellPadding.Bottom; l++)
            {
                wr.WriteLine(emptyLine);
            }
        }

        foreach (var rowCells in rows)
        {
            wr.WriteLine(middleLine);

            if (CellPadding != null)
            {
                for (var l = 0; l < CellPadding.Top; l++)
                {
                    wr.WriteLine(emptyLine);
                }
            }

            var cellsMaxHeight = 0;
            for (var height = 0; height < MaximumRowHeight; height++)
            {
                wr.Write(VerticalCharacter);
                for (var i = 0; i < columnsCount; i++)
                {
                    if (leftPadding != null && Columns[i].IsHorizontallyPadded)
                    {
                        wr.Write(leftPadding);
                    }

                    var cell = rowCells[i];
                    if (height == 0)
                    {
                        cell.ComputeTextLines();
                        if (cell.TextLines != null && cell.TextLines.Length > cellsMaxHeight)
                        {
                            cellsMaxHeight = Math.Min(MaximumRowHeight, cell.TextLines.Length);
                        }
                    }

                    if (cell.TextLines != null && height < cell.TextLines.Length)
                    {
                        cell.WriteTextLine(wr, height);
                    }
                    else
                    {
                        wr.Write(new string(' ', Columns[i].WidthWithoutPadding));
                    }

                    if (rightPadding != null && Columns[i].IsHorizontallyPadded)
                    {
                        wr.Write(rightPadding);
                    }
                    wr.Write(VerticalCharacter);
                }
                wr.WriteLine();

                if ((height + 1) == cellsMaxHeight)
                    break;
            }

            if (CellPadding != null)
            {
                for (var l = 0; l < CellPadding.Bottom; l++)
                {
                    wr.WriteLine(emptyLine);
                }
            }
        }

        wr.WriteLine(bottomLine.ToString());
    }

    protected virtual int ComputeColumnWidths(TextWriter writer, IEnumerable enumerable, IList<TableStringCell> header, IList<TableStringCell[]> rows)
    {
        if (writer == null)
            throw new ArgumentNullException(nameof(writer));

        if (enumerable == null)
            throw new ArgumentNullException(nameof(enumerable));

        if (header == null)
            throw new ArgumentNullException(nameof(header));

        if (rows == null)
            throw new ArgumentNullException(nameof(rows));

        header.Clear();
        rows.Clear();
        int[]? desiredPaddedColumnWidths = null; // with h padding
        var hp = CellPadding != null ? CellPadding.Horizontal : 0;
        foreach (var row in enumerable)
        {
            if (Columns.Count == 0)
            {
                // create the columns with the first non-null row that will create at least one column
                if (row == null)
                    continue;

                AddColumns(row);
                if (Columns.Count == 0)
                    continue;

                desiredPaddedColumnWidths = new int[Math.Min(Columns.Count, MaximumNumberOfColumns)];

                // compute header rows
                for (var i = 0; i < desiredPaddedColumnWidths.Length; i++)
                {
                    var cell = CreateCell(Columns[i], Columns[i]);
                    header.Add(cell);
                    cell.ComputeText();

                    var size = cell.DesiredColumnWith;
                    if (size != int.MaxValue && hp > 0)
                    {
                        size += hp;
                    }

                    if (size > desiredPaddedColumnWidths[i])
                    {
                        desiredPaddedColumnWidths[i] = size;
                    }
                }
            }

            if (desiredPaddedColumnWidths == null)
                continue;

            var cells = new TableStringCell[desiredPaddedColumnWidths.Length];
            for (var i = 0; i < desiredPaddedColumnWidths.Length; i++)
            {
                var value = Columns[i].GetValueFunc(Columns[i], row);
                cells[i] = CreateCell(Columns[i], value);
                cells[i].ComputeText();

                var size = cells[i].DesiredColumnWith;
                if (size != int.MaxValue && hp > 0)
                {
                    size += hp;
                }

                if (size > desiredPaddedColumnWidths[i])
                {
                    desiredPaddedColumnWidths[i] = size;
                }
            }

            rows.Add(cells);
        }

        if (desiredPaddedColumnWidths == null) // no columns
            return 0;

        if (MaximumWidth <= 0)
        {
            for (var i = 0; i < desiredPaddedColumnWidths.Length; i++)
            {
                Columns[i].WidthWithPadding = desiredPaddedColumnWidths[i];
                Columns[i].WidthWithoutPadding = Columns[i].WidthWithPadding - hp;
            }
        }
        else
        {
            for (var i = 0; i < desiredPaddedColumnWidths.Length; i++)
            {
                Columns[i].DesiredPaddedWidth = desiredPaddedColumnWidths[i];
                Columns[i].WidthWithoutPadding = Columns[i].WidthWithPadding - hp;
            }

            var borderWidth = _columnBorderWidth + desiredPaddedColumnWidths.Length * _columnBorderWidth;
            var maxWidth = MaximumWidth - Indent - borderWidth;

            // this is a small trick. When we may be outputing to the console with another textwriter, 
            // just remove one to avoid the auto WriteLine effect from the console
            if (!IsInConsoleMode(writer) && ConsoleWindowWidth == MaximumWidth)
            {
                maxWidth--;
            }

            var desiredWidth = desiredPaddedColumnWidths.Sum();
            if (desiredWidth > maxWidth)
            {
                if (CanReduceCellPadding)
                {
                    var diff = desiredWidth - maxWidth;

                    // remove padding from last column to first
                    for (var i = desiredPaddedColumnWidths.Length - 1; i >= 0; i--)
                    {
                        Columns[i].IsHorizontallyPadded = false;
                        diff -= hp;
                        if (diff <= 0)
                            break;
                    }
                }

                var availableWidth = maxWidth;
                do
                {
                    var uncomputedColumns = Columns.Take(desiredPaddedColumnWidths.Length).Where(c => c.WidthWithPadding < 0).ToArray();
                    if (uncomputedColumns.Length == 0)
                        break;

                    var avgWidth = availableWidth / uncomputedColumns.Length;
                    var computed = 0;
                    foreach (var column in uncomputedColumns)
                    {
                        if (desiredPaddedColumnWidths[column.Index] <= avgWidth)
                        {
                            column.WidthWithPadding = desiredPaddedColumnWidths[column.Index];
                            column.WidthWithoutPadding = column.WidthWithPadding - hp;
                            if (!Columns[column.Index].IsHorizontallyPadded)
                            {
                                column.WidthWithPadding -= hp;
                            }
                            availableWidth -= column.WidthWithPadding;
                            computed++;
                        }
                    }

                    if (computed == 0)
                    {
                        avgWidth = availableWidth / uncomputedColumns.Length;
                        foreach (var column in uncomputedColumns)
                        {
                            column.WidthWithPadding = avgWidth;
                            column.WidthWithoutPadding = column.WidthWithPadding;
                            if (Columns[column.Index].IsHorizontallyPadded)
                            {
                                column.WidthWithPadding += hp;
                            }
                            availableWidth -= column.WidthWithPadding;
                        }
                    }
                }
                while (true);

                // now, because of roundings and unpaddings, we may have some leftovers to distribute
                // do that in a round robbin fashion for all columns that need it
                var totalWidth = Columns.Take(desiredPaddedColumnWidths.Length).Sum(c => c.WidthWithPadding);
                if (totalWidth < maxWidth)
                {
                    var columns = Columns.Take(desiredPaddedColumnWidths.Length).Where(c => c.WidthWithPadding < c.DesiredPaddedWidth).OrderBy(c => c.WidthWithPadding).ToArray();
                    if (columns.Length > 0) // we shoull always pass here, but...
                    {
                        var index = 0;
                        for (var i = 0; i < maxWidth - totalWidth; i++)
                        {
                            Columns[index].WidthWithPadding++;
                            Columns[index].WidthWithoutPadding++;
                            index++;
                            if (index == columns.Length)
                            {
                                index = 0;
                            }
                        }
                    }
                }
            }
            else
            {
                for (var i = 0; i < desiredPaddedColumnWidths.Length; i++)
                {
                    Columns[i].WidthWithPadding = desiredPaddedColumnWidths[i];
                    Columns[i].WidthWithoutPadding = Columns[i].WidthWithPadding - hp;
                }
            }
        }
        return desiredPaddedColumnWidths.Length;
    }

    protected virtual TableStringColumn CreateColumn(string name, Func<TableStringColumn, object, object?> getValueFunc) => new(this, name, getValueFunc);
    public virtual bool IsInConsoleMode(TextWriter writer) => IsConsoleValid && (writer == Console.Out || writer is ConsoleModeTextWriter);

    public virtual void WriteWithColor(TextWriter writer, ConsoleColor foreground, string? text) => WriteWithColor(writer, foreground, Console.BackgroundColor, text);
    public virtual void WriteWithColor(TextWriter writer, ConsoleColor foreground, ConsoleColor background, string? text)
    {
        var fcolor = Console.ForegroundColor;
        var bcolor = Console.BackgroundColor;

        try
        {
            Console.ForegroundColor = foreground;
            Console.BackgroundColor = background;
            writer.Write(text);
        }
        finally
        {
            Console.ForegroundColor = fcolor;
            Console.BackgroundColor = bcolor;
        }
    }

    protected virtual bool ScanProperties(object first)
    {
        if (first == null)
            throw new ArgumentNullException(nameof(first));

        if (first is Guid || first is TimeSpan || first is DateTimeOffset || first is Uri)
            return false;

        var tc = Type.GetTypeCode(first.GetType());
        if (tc == TypeCode.Object)
            return true;

        return false;
    }

    protected virtual void AddColumns(object first)
    {
        if (first == null)
            throw new ArgumentNullException(nameof(first));

        var scanObject = ScanProperties(first);
        if (scanObject)
        {
            if (first is Array array)
            {
                for (var i = 0; i < array.Length; i++)
                {
                    AddColumn(new ArrayItemTableStringColumn(this, i));
                }
                return;
            }

            if (first is System.Data.DataRow row)
            {
                foreach (System.Data.DataColumn column in row.Table.Columns)
                {
                    AddColumn(new DataColumnTableStringColumn(this, column));
                }
                return;
            }

            if (first is ICustomTypeDescriptor desc)
            {
                foreach (PropertyDescriptor property in desc.GetProperties())
                {
                    if (!property.IsBrowsable)
                        continue;

                    AddColumn(new PropertyDescriptorTableStringColumn(this, property));
                }
                return;
            }

            if (IsKeyValuePairEnumerable(first.GetType(), out var keyType, out var valueType, out var enumerableType))
            {
                var enumerable = (IEnumerable?)Cast(enumerableType, first);
                if (enumerable != null && keyType != null && valueType != null)
                {
                    foreach (var kvp in enumerable)
                    {
                        var pi = kvp.GetType().GetProperty("Key");
                        var key = pi?.GetValue(kvp)?.ToString();
                        if (key == null)
                            continue;

                        AddColumn(new KeyValuePairTableStringColumn(this, keyType, valueType, key));
                    }
                }
                return;
            }

            foreach (var property in first.GetType().GetProperties())
            {
                var browsable = property.GetCustomAttribute<BrowsableAttribute>();
                if (browsable != null && !browsable.Browsable)
                    continue;

                if ((property.GetAccessors().FirstOrDefault()?.IsStatic).GetValueOrDefault())
                    continue;

                if (!property.CanRead)
                    continue;

                if (property.GetIndexParameters().Length > 0)
                    continue;

                AddColumn(new PropertyInfoTableStringColumn(this, property));
            }
        }

        // no columns? ok let's use the object itself (it'll be a one line table)
        if (Columns.Count == 0)
        {
            AddColumn(new ValueTableStringColumn(this));
        }
    }

    internal static object? Cast(Type? type, object? value)
    {
        if (type == null || value == null)
            return null;

        var parameter = Expression.Parameter(typeof(object));
        var block = Expression.Block(Expression.Convert(Expression.Convert(parameter, value.GetType()), type));
        var func = Expression.Lambda(block, parameter).Compile();
        return func.DynamicInvoke(value);
    }

    private static bool IsKeyValuePairEnumerable(Type inputType, out Type? keyType, out Type? valueType, out Type? enumerableType)
    {
        keyType = null;
        valueType = null;
        enumerableType = null;
        foreach (var type in inputType.GetInterfaces().Where(i => i.IsGenericType && typeof(IEnumerable<>).IsAssignableFrom(i.GetGenericTypeDefinition())))
        {
            var args = type.GetGenericArguments();
            if (args.Length != 1)
                continue;

            var kvp = args[0];
            if (!kvp.IsGenericType || !typeof(KeyValuePair<,>).IsAssignableFrom(kvp.GetGenericTypeDefinition()))
                continue;

            var kvpArgs = kvp.GetGenericArguments();
            if (kvpArgs.Length == 2)
            {
                keyType = kvpArgs[0];
                valueType = kvpArgs[1];
                enumerableType = type;
                return true;
            }
        }
        return false;
    }

    protected virtual TableStringCell CreateCell(TableStringColumn column, object? value)
    {
        if (value != null && value.Equals(column))
            return new HeaderTableStringCell(column);

        if (value is byte[] bytes)
            return new TableStringCell(column, bytes);

        return new TableStringCell(column, value);
    }

    public virtual char ToPrintable(char c)
    {
        var pf = PrintCharFunc;
        if (pf != null)
            return pf(c);

        if (c >= 32 && c <= 127)
            return c;

        return '.';
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

public enum TableStringAlignment
{
    Right,
    Left,
    Center,
}

public class TableStringColumn
{
    private int? _maxLength;
    private IFormatProvider? _formatProvider;
    private TableStringAlignment? _aligment;
    private TableStringAlignment? _headerAligment;
    private string? _hyphens;
    private int _width = -1;
    private int _widthWithoutPadding = -1;
    private bool? _padded;
    private char? _newLineReplacement;
    private char? _nonPrintableReplacement;
    private ConsoleColor? _headerForegroundColor;
    private ConsoleColor? _headerBackgroundColor;
    private ConsoleColor? _foregroundColor;
    private ConsoleColor? _backgroundColor;

    public TableStringColumn(TableString table, string name, Func<TableStringColumn, object, object?> getValueFunc)
    {
        Table = table ?? throw new ArgumentNullException(nameof(table));
        GetValueFunc = getValueFunc ?? throw new ArgumentNullException(nameof(getValueFunc));
        Name = name ?? throw new ArgumentNullException(nameof(name));
        WidthWithPadding = -1;
    }

    public TableString Table { get; }
    public string Name { get; }
    public Func<TableStringColumn, object, object?> GetValueFunc { get; }
    public int Index { get; internal set; }
    public int DesiredPaddedWidth { get; internal set; }
    public int WidthWithPadding { get => _width; internal set => _width = Math.Min(MaxLength, value); }
    public int WidthWithoutPadding { get => _widthWithoutPadding; internal set => _widthWithoutPadding = Math.Min(MaxLength, value); }
    public bool IsHorizontallyPadded { get => _padded ?? true; internal set => _padded = value; }

    public virtual int MaxLength { get => _maxLength ?? Table.DefaultCellMaxLength; set => _maxLength = value; }
    public virtual string? Hyphens { get => _hyphens ?? Table.DefaultHyphens; set => _hyphens = value; }
    public virtual char NewLineReplacement { get => _newLineReplacement ?? Table.DefaultNewLineReplacement; set => _newLineReplacement = value; }
    public virtual char NonPrintableReplacement { get => _nonPrintableReplacement ?? Table.DefaultNonPrintableReplacement; set => _nonPrintableReplacement = Table.ToPrintable(value); }
    public virtual IFormatProvider? FormatProvider { get => _formatProvider ?? Table.DefaultFormatProvider; set => _formatProvider = value; }
    public virtual TableStringAlignment Alignment { get => _aligment ?? Table.DefaultCellAlignment; set => _aligment = value; }
    public virtual TableStringAlignment HeaderAlignment { get => _headerAligment ?? Table.DefaultHeaderCellAlignment; set => _headerAligment = value; }
    public virtual ConsoleColor? HeaderForegroundColor { get => _headerForegroundColor ?? Table.DefaultHeaderForegroundColor; set => _headerForegroundColor = value; }
    public virtual ConsoleColor? HeaderBackgroundColor { get => _headerBackgroundColor ?? Table.DefaultHeaderBackgroundColor; set => _headerBackgroundColor = value; }
    public virtual ConsoleColor? ForegroundColor { get => _foregroundColor ?? Table.DefaultForegroundColor; set => _foregroundColor = value; }
    public virtual ConsoleColor? BackgroundColor { get => _backgroundColor ?? Table.DefaultBackgroundColor; set => _backgroundColor = value; }

    public override string ToString() => Name;
}

public class TableStringCell(TableStringColumn column, object? value)
{
    private string[]? _split;

    public TableStringColumn Column { get; } = column ?? throw new ArgumentNullException(nameof(column));
    public object? Value { get; } = value;
    public virtual TableStringAlignment Alignment => Column.Alignment;
    public virtual string? Text { get; protected set; }
    public virtual string?[]? TextLines { get; protected set; }

    public virtual int DesiredColumnWith
    {
        get
        {
            if (Text == null)
                return 0;

            var pos = Text.IndexOfAny(['\r', '\n']);
            if (pos >= 0)
            {
                _split ??= Text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
                return _split.Max(s => s.Length);
            }

            if (Column.MaxLength <= 0)
                return Text.Length;

            return Math.Min(Text.Length, Column.MaxLength);
        }
    }

    public override string? ToString() => Text;

    public virtual void ComputeText()
    {
        if (Value is not string s)
        {
            s = string.Format(Column.FormatProvider, "{0}", Value);
        }
        Text = EscapeText(s);
    }

    // this early escaping can change text length
    public virtual string? EscapeText(string text)
    {
        if (string.IsNullOrEmpty(text))
            return text;

        var sb = new StringBuilder(text.Length);
        foreach (var c in text)
        {
            if (c == '\t')
            {
                sb.Append(Column.Table.TabString);
                continue;
            }
            sb.Append(c);
        }
        return sb.ToString();
    }

    // this *must not* change text length, it's too late
    public virtual string? EscapeTextLine(string text)
    {
        if (string.IsNullOrEmpty(text))
            return text;

        var escaped = new char[text.Length];
        for (var i = 0; i < text.Length; i++)
        {
            escaped[i] = Column.Table.ToPrintable(text[i]);
        }
        return new string(escaped);
    }

    public virtual void WriteTextLine(TextWriter writer, int index)
    {
        if (writer == null)
            throw new ArgumentNullException(nameof(writer));

        if (TextLines == null)
            throw new InvalidOperationException();

        if (Column.Table.IsInConsoleMode(writer) && (Column.ForegroundColor.HasValue || Column.BackgroundColor.HasValue))
        {
            Column.Table.WriteWithColor(writer, Column.ForegroundColor ?? Console.ForegroundColor, Column.BackgroundColor ?? Console.BackgroundColor, TextLines[index]);
            return;
        }

        writer.Write(TextLines[index]);
    }

    public virtual void ComputeTextLines()
    {
        if (TextLines != null)
            return;

        if (Text == null)
        {
            TextLines = [null];
        }
        else if (_split == null && Text.Length <= Column.WidthWithoutPadding)
        {
            TextLines = [Align(EscapeTextLine(Text))];
        }
        else
        {
            var split = _split ?? Text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
            var lines = new List<string?>();
            var segmentWidth = Column.WidthWithoutPadding - 1; // keep 1 char to display NewLineReplacement
            for (var i = 0; i < split.Length; i++)
            {
                var line = split[i];

                if (Column.Table.CellWrap)
                {
                    var pos = 0;
                    do
                    {
                        if (pos + segmentWidth >= line.Length || (split.Length == 1 && split[0].Length <= Column.WidthWithoutPadding))
                        {
                            lines.Add(Align(EscapeTextLine(line.Substring(pos))));
                            break;
                        }

                        if (line.Length == Column.WidthWithoutPadding)
                        {
                            lines.Add(Align(EscapeTextLine(line)));
                            break;
                        }
                        else
                        {
                            var dline = line.Substring(pos, segmentWidth);
                            lines.Add(Align(EscapeTextLine(dline) + Column.NewLineReplacement));
                        }
                        pos += segmentWidth;
                    }
                    while (true);
                }
                else
                {
                    string dline;
                    if (line.Length <= Column.WidthWithoutPadding)
                    {
                        dline = line;
                    }
                    else
                    {
                        if (Column.Hyphens != null && Column.Hyphens.Length <= Column.WidthWithoutPadding)
                        {
                            dline = line.Substring(0, Column.WidthWithoutPadding - Column.Hyphens.Length) + Column.Hyphens;
                        }
                        else
                        {
                            dline = line.Substring(0, Column.WidthWithoutPadding);
                        }
                    }

                    // add hyphens to the last line if needed
                    if (Column.Hyphens != null && lines.Count == Column.Table.MaximumRowHeight - 1)
                    {
                        if (dline.Length < Column.WidthWithoutPadding)
                        {
                            dline += Column.Hyphens;
                        }
                        else
                        {
                            dline = dline.Substring(0, dline.Length - Column.Hyphens.Length) + Column.Hyphens;
                        }
                    }

                    lines.Add(Align(EscapeTextLine(dline)));
                }

                if (lines.Count == Column.Table.MaximumRowHeight)
                    break;
            }

            TextLines = [.. lines];
        }
    }

    protected virtual string? Align(string? text)
    {
        string? str;
        switch (Alignment)
        {
            case TableStringAlignment.Left:
                str = string.Format("{0,-" + Column.WidthWithoutPadding + "}", text);
                break;

            case TableStringAlignment.Center:
                var spaces = Column.WidthWithoutPadding - (text != null ? text.Length : 0);
                if (spaces == 0)
                {
                    str = text;
                }
                else
                {
                    var left = spaces - spaces / 2;
                    var right = spaces - left;
                    str = new string(' ', left) + text + new string(' ', right);
                }
                break;

            default:
                str = string.Format("{0," + Column.WidthWithoutPadding + "}", text);
                break;

        }
        return str;
    }
}

public class HeaderTableStringCell(TableStringColumn column) : TableStringCell(column, column?.Name)
{
    public override TableStringAlignment Alignment => Column.HeaderAlignment;

    public override void WriteTextLine(TextWriter writer, int index)
    {
        if (writer == null)
            throw new ArgumentNullException(nameof(writer));

        if (TextLines == null)
            throw new InvalidOperationException();

        if (Column.Table.IsInConsoleMode(writer) && (Column.HeaderForegroundColor.HasValue || Column.HeaderBackgroundColor.HasValue))
        {
            Column.Table.WriteWithColor(writer, Column.HeaderForegroundColor ?? Console.ForegroundColor, Column.HeaderBackgroundColor ?? Console.BackgroundColor, TextLines[index]);
            return;
        }

        writer.Write(TextLines[index]);
    }
}

public class ByteArrayTableStringCell(TableStringColumn column, byte[] bytes) : TableStringCell(column, bytes)
{
    public new byte[]? Value => (byte[]?)base.Value;

    public override void ComputeText()
    {
        var bytes = Value;
        var max = Column.Table.MaximumByteArrayDisplayCount;

        if (bytes == null)
        {
            Text = string.Empty;
            return;
        }

        if (bytes.Length == 0)
        {
            Text = "0x";
            return;
        }

        if (bytes.Length > max)
        {
            Text = "0x" + BitConverter.ToString(bytes, 0, max).Replace("-", string.Empty) + " (" + bytes.Length + ") " + Column.Hyphens;
            return;
        }

        Text = "0x" + BitConverter.ToString(bytes, 0, bytes.Length).Replace("-", string.Empty);
    }
}

public class ObjectTableString(object? obj) : TableString
{
    public bool AddValueTypeColumn { get; set; }
    public bool AddArrayRow { get; set; } = true;
    public bool ExpandEnumerable { get; set; } = true;
    public object? Object { get; } = obj;

    internal static object? GetValue(PropertyInfo property, object obj, bool throwOnError)
    {
        object? value;
        if (throwOnError)
        {
            value = property.GetValue(obj);
            if (value is IEnumerable enumerable)
                return GetValue(enumerable);

        }
        else
        {
            try
            {
                value = property.GetValue(obj);
                if (value is IEnumerable enumerable)
                    return GetValue(enumerable);
            }
            catch (Exception e)
            {
                value = "#ERR: " + e.Message;
            }
        }
        return value;
    }

    private static string GetValue(IEnumerable enumerable)
    {
        if (enumerable is string s)
            return s;

        return string.Join(Environment.NewLine, enumerable.Cast<object>());
    }

    protected virtual IEnumerable<Tuple<object?, object?>> Values
    {
        get
        {
            var list = new List<Tuple<object?, object?>>();
            var i = 0;
            var array = Object as Array;
            if (Object != null && Object is not string)
            {
                foreach (var property in Object.GetType().GetProperties())
                {
                    if (!property.CanRead)
                        continue;

                    if ((property.GetAccessors().FirstOrDefault()?.IsStatic).GetValueOrDefault())
                        continue;

                    if (property.GetIndexParameters().Length > 0)
                        continue;

                    var browsable = property.GetCustomAttribute<BrowsableAttribute>();
                    if (browsable != null && !browsable.Browsable)
                        continue;

                    // this one will cause unwanted array dumps
                    if (array != null && string.Equals(property.Name, nameof(Array.SyncRoot), StringComparison.Ordinal))
                        continue;

                    var value = GetValue(property, Object, ThrowOnPropertyGetError);
                    list.Add(new Tuple<object?, object?>(property.Name, value));
                    i++;
                }

                // sort by property name
                list.Sort(new ComparableComparer());
            }

            // no columns? let's return the object itself (we support null)
            if (i == 0)
            {
                list.Add(new Tuple<object?, object?>(Object?.GetType(), Object));
            }
            else if (AddArrayRow && array != null)
            {
                list.Add(new Tuple<object?, object?>("<values>", string.Join(Environment.NewLine, array.Cast<object>())));
            }
            return list;
        }
    }

    protected class ComparableComparer : IComparer<Tuple<object?, object?>>
    {
        public int Compare(Tuple<object?, object?>? x, Tuple<object?, object?>? y) => (((IComparable?)x?.Item1)?.CompareTo((IComparable?)y?.Item1)).GetValueOrDefault();
    }

    protected override void AddColumns(object first)
    {
        var firstColumnName = "Name";
        var item1 = ((Tuple<object?, object?>)first).Item1;
        if (item1 == null || item1 is Type)
        {
            firstColumnName = "Type";
        }

        var nameColumn = CreateColumn(firstColumnName, (c, r) => ((Tuple<object?, object?>)r).Item1 ?? "<null>") ?? throw new InvalidOperationException();
        nameColumn.HeaderAlignment = TableStringAlignment.Left;
        nameColumn.Alignment = nameColumn.HeaderAlignment;
        AddColumn(nameColumn);

        var valueColumn = CreateColumn("Value", (c, r) => ((Tuple<object?, object?>)r).Item2) ?? throw new InvalidOperationException();
        AddColumn(valueColumn);

        if (AddValueTypeColumn)
        {
            var typeColumn = CreateColumn("Type", (c, r) =>
            {
                var value = ((Tuple<object?, object?>)r).Item2;
                if (value == null)
                    return null;

                return value.GetType().FullName;
            }) ?? throw new InvalidOperationException();
            AddColumn(typeColumn);
        }
    }

    public void WriteObject(TextWriter writer) => Write(writer, Values);
    public virtual string WriteObject()
    {
        using var sw = new StringWriter();
        WriteObject(sw);
        return sw.ToString();
    }
}

public class StructTableString(object obj) : ObjectTableString(obj)
{
    protected override IEnumerable<Tuple<object?, object?>> Values
    {
        get
        {
            var list = new List<Tuple<object?, object?>>();
            var i = 0;
            if (Object != null && Object is not string)
            {
                foreach (var fld in Object.GetType().GetFields(BindingFlags.Public | BindingFlags.Instance))
                {
                    var browsable = fld.GetCustomAttribute<BrowsableAttribute>();
                    if (browsable != null && !browsable.Browsable)
                        continue;

                    var value = GetValue(fld, Object, ThrowOnPropertyGetError);
                    list.Add(new Tuple<object?, object?>(fld.Name, value));
                    i++;
                }

                list.Sort(new ComparableComparer());
            }

            if (i == 0)
            {
                list.Add(new Tuple<object?, object?>(Object?.GetType(), Object));
            }
            return list;
        }
    }

    internal static object? GetValue(FieldInfo field, object obj, bool throwOnError)
    {
        object? value;
        if (throwOnError)
        {
            value = field.GetValue(obj);
        }
        else
        {
            try
            {
                value = field.GetValue(obj);
            }
            catch (Exception e)
            {
                value = "#ERR: " + e.Message;
            }
        }
        return value;
    }
}

public class ValueTableStringColumn(TableString table) : TableStringColumn(table, "Value", (c, r) => r)
{
}

public class ArrayItemTableStringColumn(TableString table, int index) : TableStringColumn(table, "#" + index.ToString(CultureInfo.CurrentCulture), (c, r) => ((Array)r).GetValue(((ArrayItemTableStringColumn)c).ArrayIndex))
{
    public int ArrayIndex { get; } = index;
}

public class KeyValuePairTableStringColumn : TableStringColumn
{
    public KeyValuePairTableStringColumn(TableString table, Type keyType, Type valueType, string name)
        : base(table, name, (c, r) =>
        {
            var objs = new object?[] { name, null };
            var b = (bool)((KeyValuePairTableStringColumn)c).Method.Invoke(r, objs)!;
            return b ? objs[1] : null;
        })
    {
        if (keyType == null)
            throw new ArgumentNullException(nameof(keyType));

        if (valueType == null)
            throw new ArgumentNullException(nameof(valueType));

        var type = typeof(IDictionary<,>).MakeGenericType(keyType, valueType);
        Method = type.GetMethod("TryGetValue")!;
        if (Method == null)
            throw new NotSupportedException();
    }

    public MethodInfo Method { get; }
}

public class DataColumnTableStringColumn(TableString table, System.Data.DataColumn dataColumn) : TableStringColumn(table, dataColumn?.ColumnName!, (c, r) => ((System.Data.DataRow)r)[((DataColumnTableStringColumn)c).DataColumn])
{
    public System.Data.DataColumn DataColumn { get; } = dataColumn!;
}

public class PropertyDescriptorTableStringColumn(TableString table, PropertyDescriptor property) : TableStringColumn(table, property?.Name!, (c, r) => ((PropertyDescriptorTableStringColumn)c).GetValue(r))
{
    public PropertyDescriptor Property { get; } = property!;

    private object? GetValue(object component)
    {
        try
        {
            return Property.GetValue(component);
        }
        catch
        {
            return null;
        }
    }
}

public class PropertyInfoTableStringColumn(TableString table, PropertyInfo property) : TableStringColumn(table, property?.Name!, (c, r) => ObjectTableString.GetValue(((PropertyInfoTableStringColumn)c).Property, r, table.ThrowOnPropertyGetError))
{
    public PropertyInfo Property { get; } = property!;
}

public static class TableStringExtensions
{
    public static string ToTableString<T>(this IEnumerable<T>? enumerable) => new TableString().Write(enumerable);
    public static string ToTableString(this IEnumerable? enumerable) => new TableString().Write(enumerable);

    public static string ToTableString<T>(this IEnumerable<T>? enumerable, int indent)
    {
        var ts = new TableString
        {
            Indent = indent
        };
        return ts.Write(enumerable);
    }

    public static string ToTableString(this IEnumerable? enumerable, int indent)
    {
        var ts = new TableString
        {
            Indent = indent
        };
        return ts.Write(enumerable);
    }

    public static void ToTableString<T>(this IEnumerable<T>? enumerable, TextWriter writer) => new TableString().Write(writer, enumerable);
    public static void ToTableString(this IEnumerable? enumerable, TextWriter writer) => new TableString().Write(writer, enumerable);
    public static void ToTableString<T>(this IEnumerable<T>? enumerable, TextWriter writer, int indent)
    {
        var ts = new TableString
        {
            Indent = indent
        };
        ts.Write(writer, enumerable);
    }

    public static void ToTableString(this IEnumerable enumerable, TextWriter writer, int indent)
    {
        var ts = new TableString
        {
            Indent = indent
        };
        ts.Write(writer, enumerable);
    }

    public static string ToTableString(object? obj, int indent) => new ObjectTableString(obj) { Indent = indent }.WriteObject();
    public static string ToTableString(object? obj) => new ObjectTableString(obj).WriteObject();
    public static void ToTableString(object? obj, TextWriter writer, int indent) => new ObjectTableString(obj) { Indent = indent }.WriteObject(writer);
    public static void ToTableString(object? obj, TextWriter writer) => new ObjectTableString(obj).WriteObject(writer);
}
