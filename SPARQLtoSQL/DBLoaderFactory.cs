/* 
Copyright © 2017 Yurii Bilyk. All rights reserved. Contacts: <yuryk531@gmail.com>

This file is part of "Database integrator".

"Database integrator" is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

"Database integrator" is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with "Database integrator".  If not, see <http:www.gnu.org/licenses/>. 
*/
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
