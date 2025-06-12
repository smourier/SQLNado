namespace SqlNado;

public class SQLiteToken
{
    public SQLiteToken(string text, int startOffset, int endOffset, int position)
    {
        if (text == null)
            throw new ArgumentNullException(nameof(text));

        if (startOffset < 0)
            throw new ArgumentException(null, nameof(startOffset));

        if (endOffset < 0 || endOffset < startOffset)
            throw new ArgumentException(null, nameof(endOffset));

        if (position < 0)
            throw new ArgumentException(null, nameof(position));

        Text = text;
        StartOffset = startOffset;
        EndOffset = endOffset;
        Position = position;
    }

    public string Text { get; }
    public int StartOffset { get; }
    public int EndOffset { get; }
    public int Position { get; }

    public override string ToString() => Text;
}
