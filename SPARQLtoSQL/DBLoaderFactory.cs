using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SPARQLtoSQL
{
    public class DBLoaderFactory : IDBLoaderFactory
    {
        static Dictionary<string, Type> dbConStringLoaderDict = new Dictionary<string, Type>();

        public static void RegisterDBLoaders(Type dbLoader, params string[] connStrings)
        {
            if (dbLoader.GetInterface("IDBLoader") != null)
            {
                foreach (var cStr in connStrings)
                {
                    if(!dbConStringLoaderDict.ContainsKey(cStr))
                    {
                        AddDBLoader(cStr, dbLoader);
                    }
                }
            }
            else throw new ArgumentException($"dbLoader class type must implement IDBLoader!");
        }

        private static void AddDBLoader(string connString, Type dbLoader)
        {
            if (connString.Length > 0)
            {
                dbConStringLoaderDict.Add(connString, dbLoader);
            }
            else throw new ArgumentException("Connection string couldn't be empty!");
        }

        public IDBLoader GetDBLoader(string connString)
        {
            if (dbConStringLoaderDict.ContainsKey(connString))
            {
                IDBLoader dbLoader = Activator.CreateInstance(dbConStringLoaderDict[connString]) as IDBLoader;
                dbLoader.ConnectionString = connString;
                return dbLoader;
            }
            else throw new ArgumentException($"A DB loader is not registered for this connection string ({connString})!");
        }
    }
}
