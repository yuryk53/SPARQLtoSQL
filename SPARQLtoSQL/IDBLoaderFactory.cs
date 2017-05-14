namespace SPARQLtoSQL
{
    public interface IDBLoaderFactory
    {
        IDBLoader GetDBLoader(string connString);
    }
}