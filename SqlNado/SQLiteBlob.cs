namespace SqlNado;

public class SQLiteBlob : IDisposable
{
    private IntPtr _handle;

    public SQLiteBlob(SQLiteDatabase database, IntPtr handle, string tableName, string columnName, long rowId, SQLiteBlobOpenMode mode)
    {
        if (handle == IntPtr.Zero)
            throw new ArgumentException(null, nameof(handle));
        Database = database ?? throw new ArgumentNullException(nameof(database));
        _handle = handle;
        TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
        RowId = rowId;
        Mode = mode;
    }

    [Browsable(false)]
    public SQLiteDatabase Database { get; }
    [Browsable(false)]
    public IntPtr Handle => _handle;
    public string TableName { get; }
    public string ColumnName { get; }
    public long RowId { get; }
    public SQLiteBlobOpenMode Mode { get; }

    public override string ToString() => TableName + ":" + ColumnName + ":" + RowId;

    public virtual int Size => SQLiteDatabase.Native.sqlite3_blob_bytes(CheckDisposed());
    public virtual void MoveToNewRow(long rowId) => Database.CheckError(SQLiteDatabase.Native.sqlite3_blob_reopen(CheckDisposed(), rowId));
    public virtual void Read(byte[] buffer, int count, int blobOffset) => Database.CheckError(SQLiteDatabase.Native.sqlite3_blob_read(CheckDisposed(), buffer, count, blobOffset));
    public virtual void Write(byte[] buffer, int count, int blobOffset) => Database.CheckError(SQLiteDatabase.Native.sqlite3_blob_write(CheckDisposed(), buffer, count, blobOffset));

    // This is not recommended to use this, in general.
    // SQLiteBlob's design targets streams, not byte arrays. If you really want a byte array, then don't use this blob class type, just use byte[]
    public virtual byte[] ToArray()
    {
        using var ms = new MemoryStream(Size);
        CopyTo(ms);
        return ms.ToArray();
    }

    public virtual void CopyTo(Stream output)
    {
        if (output == null)
            throw new ArgumentNullException(nameof(output));

        using var blob = new BlobStream(this);
        blob.CopyTo(output);
    }

    public virtual void CopyFrom(Stream input)
    {
        if (input == null)
            throw new ArgumentNullException(nameof(input));

        using var blob = new BlobStream(this);
        input.CopyTo(blob);
    }

    protected internal IntPtr CheckDisposed()
    {
        var handle = _handle;
        if (handle == IntPtr.Zero)
            throw new ObjectDisposedException(nameof(Handle));

        return handle;
    }

    protected virtual void Dispose(bool disposing)
    {
        var handle = Interlocked.Exchange(ref _handle, IntPtr.Zero);
        if (handle != IntPtr.Zero)
        {
            SQLiteDatabase.Native.sqlite3_blob_close(handle);
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~SQLiteBlob() => Dispose(false);

    protected class BlobStream(SQLiteBlob blob) : Stream
    {
        private int _position;

        public SQLiteBlob Blob { get; } = blob ?? throw new ArgumentNullException(nameof(blob));
        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => Blob.Mode == SQLiteBlobOpenMode.ReadWrite;
        public override long Length => Blob.Size;
        public override long Position { get => _position; set => Seek(value, SeekOrigin.Begin); }

        public override void Flush()
        {
            // do nothing special
        }

        public override void SetLength(long value) => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (count <= 0)
                throw new ArgumentException(null, nameof(count));

            byte[] buf;
            if (offset == 0)
            {
                buf = buffer;
            }
            else
            {
                buf = new byte[count];
            }

            var left = Math.Min(Blob.Size - _position, count);
            Blob.Read(buf, left, _position);
            if (offset != 0)
            {
                Buffer.BlockCopy(buf, 0, buffer, offset, left);
            }
            _position += left;
            return left;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long pos = _position;
            switch (origin)
            {
                case SeekOrigin.Current:
                    pos += offset;
                    break;

                case SeekOrigin.End:
                    pos = Blob.Size + offset;
                    break;
            }

            if (pos > int.MaxValue)
            {
                pos = Blob.Size;
            }

            _position = Math.Max(Blob.Size, (int)pos);
            return _position;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (!CanWrite)
                throw new NotSupportedException();

            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (count <= 0)
                throw new ArgumentException(null, nameof(count));

            if (Blob.Size == 0) // special case we have often
                throw new SqlNadoException("0019: Blob is empty. You must first resize the blob to the exact size.");

            byte[] buf;
            if (offset == 0)
            {
                buf = buffer;
            }
            else
            {
                buf = new byte[count];
                Buffer.BlockCopy(buffer, offset, buf, 0, count);
            }

            if (_position + count > Blob.Size)
                throw new SqlNadoException("0022: Blob size (" + Blob.Size + " byte(s)) is too small to be able to write " + count + " bytes at position " + _position + ". You must first resize the blob to the exact size.");

            Blob.Write(buf, count, _position);
            _position += count;
        }
    }
}
