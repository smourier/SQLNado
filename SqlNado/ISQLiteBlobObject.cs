namespace SqlNado;

public interface ISQLiteBlobObject
{
    bool TryGetData(out byte[]? data);
}
