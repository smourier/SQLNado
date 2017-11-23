
namespace SqlNado
{
    public interface ISQLiteObjectChangeEvents
    {
        bool RaiseOnPropertyChanging { get; set; }
        bool RaiseOnPropertyChanged { get; set; }
        bool RaiseOnErrorsChanged { get; set; }
    }
}
