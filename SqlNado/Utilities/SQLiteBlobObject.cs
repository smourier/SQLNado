namespace SqlNado.Utilities;

public class SQLiteBlobObject(SQLiteBaseObject owner, string columnName) : ISQLiteBlobObject
{
    public SQLiteBaseObject Owner { get; } = owner ?? throw new ArgumentNullException(nameof(owner));
    public string ColumnName { get; } = columnName ?? throw new ArgumentNullException(nameof(columnName));

    bool ISQLiteBlobObject.TryGetData(out byte[]? data)
    {
        data = null;
        return false;
    }

    public int Save(byte[] inputData) => Save(inputData, -1);
    public int Save(byte[] inputData, long rowId)
    {
        if (inputData == null)
            throw new ArgumentNullException(nameof(inputData));

        using var ms = new MemoryStream(inputData);
        return Save(ms, rowId);
    }

    public int Save(string inputFilePath) => Save(inputFilePath, -1);
    public virtual int Save(string inputFilePath, long rowId)
    {
        if (inputFilePath == null)
            throw new ArgumentNullException(nameof(inputFilePath));

        using var file = File.OpenRead(inputFilePath);
        return Save(file, rowId);
    }

    public int Save(Stream inputStream) => Save(inputStream, -1);
    public virtual int Save(Stream inputStream, long rowId)
    {
        if (inputStream == null)
            throw new ArgumentNullException(nameof(inputStream));

        long length;
        try
        {
            length = inputStream.Length;
        }
        catch (Exception e)
        {
            throw new SqlNadoException("0017: Input stream must support calling the Length property to use this method.", new ArgumentException(null, nameof(inputStream), e));
        }

        if (length > int.MaxValue)
            throw new ArgumentNullException(nameof(inputStream));

        var db = ((ISQLiteObject)Owner).Database;
        var table = db!.GetObjectTable(Owner.GetType());
        var col = table.GetColumn(ColumnName) ?? throw new SqlNadoException("0018: Cannot find column name '" + ColumnName + "' on table '" + table.Name + "'.'");
        if (rowId < 0)
        {
            rowId = table.GetRowId(Owner);
        }

        var len = (int)length;
        var blen = db.GetBlobSize(table.Name, col.Name, rowId);
        if (blen != len)
        {
            db.ResizeBlob(table.Name, col.Name, rowId, len);
        }

        using (var blob = db.OpenBlob(table.Name, col.Name, rowId, SQLiteBlobOpenMode.ReadWrite))
        {
            if (blob.Size != len)
                throw new SqlNadoException("0020: Blob size is unexpected: " + blob.Size + ", expected: " + len);

            blob.CopyFrom(inputStream);
        }
        return len;
    }

    public byte[] ToArray() => ToArray(-1);
    public byte[] ToArray(long rowId)
    {
        using var ms = new MemoryStream();
        Load(ms, rowId);
        return ms.ToArray();
    }

    public int Load(string outputFilePath) => Load(outputFilePath, -1);
    public virtual int Load(string outputFilePath, long rowId)
    {
        if (outputFilePath == null)
            throw new ArgumentNullException(nameof(outputFilePath));

        using var file = File.OpenWrite(outputFilePath);
        return Load(file, rowId);
    }

    public int Load(Stream outputStream) => Load(outputStream, -1);
    public virtual int Load(Stream outputStream, long rowId)
    {
        if (outputStream == null)
            throw new ArgumentNullException(nameof(outputStream));

        var db = ((ISQLiteObject)Owner).Database;
        var table = db!.GetObjectTable(Owner.GetType());
        var col = table.GetColumn(ColumnName) ?? throw new SqlNadoException("0021: Cannot find column name '" + ColumnName + "' on table '" + table.Name + "'.'");
        if (rowId < 0)
        {
            rowId = table.GetRowId(Owner);
        }

        using var blob = db.OpenBlob(table.Name, col.Name, rowId);
        blob.CopyTo(outputStream);
        return blob.Size;
    }
}
