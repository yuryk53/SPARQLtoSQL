using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;

namespace SPARQLtoSQL
{
    public class RawTriple
    {
        public string Subj { get; set; }
        public string Pred { get; set; }
        public string Obj { get; set; }
        public override string ToString()
        {
            return $"{Subj} {Pred} {Obj}";
        }
    }

    public class MSSQLdBLoader : IDBLoader
    {
        string connString;

        string IDBLoader.ConnectionString
        {
            get
            {
                return connString;
            }

            set
            {
                connString = value;
            }
        }

        public MSSQLdBLoader() { }

        public MSSQLdBLoader(string connString)
        {
            this.connString = connString;
        }

        public List<string> GetTableNames(bool includeViews = false)
        {
            List<string> tables = new List<string>();

            using (SqlConnection conn = new SqlConnection(connString))
            {
                conn.Open();
                DataTable schema = conn.GetSchema("Tables");
                foreach (DataRow row in schema.Rows)
                {
                    tables.Add((string)row[2]);
                }

                if (!includeViews)
                {
                    schema = conn.GetSchema("Views");
                    foreach (DataRow row in schema.Rows)
                    {
                        tables.Remove((string)row[2]);
                    }
                }
            }
            tables.Remove("sysdiagrams");
            return tables;
        }

        public List<string> GetColumnNames(string tableName)
        {
            List<string> columns = new List<string>();
            using (SqlConnection conn = new SqlConnection(connString))
            {
                string[] restrictions = new string[4] { null, null, tableName, null };
                conn.Open();
                columns = conn.GetSchema("Columns", restrictions).AsEnumerable().Select(s => s.Field<String>("Column_Name")).ToList();
            }
            return columns;
        }

        public string GetDBName()
        {
            string[] connStringElements = this.connString.Split(';');
            Regex r = new Regex(@"Initial\s+Catalog\s*=\s*(\w+)");
            foreach(string element in connStringElements)
            {
                if(r.IsMatch(element))
                {
                    return r.Matches(element)[0].Groups[1].Value;
                }
            }
            throw new Exception($"Unable to locate DB Name in connection string '{connString}' ");
        }

        public List<RawTriple> GetTriplesFromTable(string tableName, params string[] tableColumns)
        {
            List<RawTriple> triples = new List<RawTriple>();
            using (SqlConnection conn = new SqlConnection(connString))
            {
                SqlCommand cmd = conn.CreateCommand();
                string columns;
                if (tableColumns.Length == 0)
                {
                    columns = string.Join(",", GetColumnNames(tableName));
                }
                else
                    columns = string.Join(",", tableColumns);

                cmd.CommandText = $"SELECT {columns} FROM [{tableName}]";
                conn.Open();
                SqlDataReader reader = cmd.ExecuteReader();
                int counter = 0; //in case there's no ID field
                while (reader.Read())
                {
                    foreach (string colName in columns.Split(','))
                    {
                        RawTriple triple = new RawTriple
                        {
                            Subj = tableName + (reader["ID"] ?? ++counter),
                            Pred = colName,
                            Obj = reader[colName].ToString()
                        };
                        triples.Add(triple);
                    }
                    triples.Add(new RawTriple { Subj = tableName + (reader["ID"] ?? ++counter), Pred = "Table", Obj = tableName });
                }
            }
            return triples;
        }

        public List<RawTriple> GetTriplesForPredicateObject_ObjProperty(string lhsTableName, string nmTableName, string rhsTableName, string prefixURI, string colName=null, string obj=null)
        {
            List<RawTriple> triples = new List<RawTriple>();
            if (obj != null)
            {
                obj = obj.Trim('"');
            }

            //n:m table should have exactly 2 foreign keys, otherwise we might get incorrect results
            string lhsPK = GetPrimaryKeys(GetDBName(), lhsTableName)[0];
            string rhsPK = GetPrimaryKeys(GetDBName(), rhsTableName)[0];

            string nmFK_lhsPK = GetFKReferencingPKTable(
                dbName: this.GetDBName(),
                pkTableName: lhsTableName,
                referencingTable: nmTableName);

            string nmFK_rhsPK = GetFKReferencingPKTable(
                dbName: this.GetDBName(),
                pkTableName: rhsTableName,
                referencingTable: nmTableName);

            using (SqlConnection conn = new SqlConnection(connString))
            {
                SqlCommand cmd = conn.CreateCommand();

                if (obj != null)
                {
                    if (colName == null)
                        throw new ArgumentNullException("Column name cannot be null, when object is specified!");

                    cmd.CommandText = $@"SELECT [{rhsTableName}].[{rhsPK}] AS rhsPK,
                                                [{lhsTableName}].[{lhsPK}] AS lhsPK
                                                   FROM [{lhsTableName}]
                                                   INNER JOIN [{nmTableName}] ON [{lhsTableName}].[{lhsPK}] = [{nmTableName}].[{nmFK_lhsPK}]
                                                   INNER JOIN [{rhsTableName}] ON [{rhsTableName}].[{rhsPK}] = [{nmTableName}].[{nmFK_rhsPK}]
                                        WHERE [{rhsTableName}].[{colName}]={obj}";
                    //colName == predicate
                }
                else
                {
                    cmd.CommandText = $@"SELECT [{rhsTableName}].[{rhsPK}] AS rhsPK,
                                                [{lhsTableName}].[{lhsPK}] AS lhsPK
                                                   FROM [{lhsTableName}]
                                                   INNER JOIN [{nmTableName}] ON [{lhsTableName}].[{lhsPK}] = [{nmTableName}].[{nmFK_lhsPK}]
                                                   INNER JOIN [{rhsTableName}] ON [{rhsTableName}].[{rhsPK}] = [{nmTableName}].[{nmFK_rhsPK}]";
                }
                conn.Open();
                SqlDataReader reader = cmd.ExecuteReader();
                int counter = 0; //in case there's no ID field
                string dbName = GetDBName();
                //string rhsPK = GetPrimaryKeys(GetDBName(), rhsTableName)[0];
                //string lhsPK = GetPrimaryKeys(GetDBName(), lhsTableName)[0];

                while (reader.Read())
                {
                    counter++;
                    object o1 = reader[0];
                    object o2 = reader[1];
                    RawTriple triple = new RawTriple
                    {
                        Subj = $"{prefixURI}{dbName}/{lhsTableName}/{lhsPK}.{(reader["lhsPK"] ?? counter)}", // dbName + tableName + (reader["ID"] ?? ++counter),
                        Pred = $"{prefixURI}{dbName}/{lhsTableName}#{nmTableName}",
                        Obj = $"{prefixURI}{dbName}/{rhsTableName}/{rhsPK}.{(reader["rhsPK"] ?? counter)}"
                    };
                    triples.Add(triple);
                }
            }
            return triples;
        }

        public List<RawTriple> GetTriplesForPredicateObject(string tableName, string columnName, string prefixURI, string obj, List<string> IFPs=null)
        {
            List<RawTriple> triples = new List<RawTriple>();
            if(obj!= null)
            {
                obj = obj.Trim('"');
            }
            using (SqlConnection conn = new SqlConnection(connString))
            {
                SqlCommand cmd = conn.CreateCommand();

                if (obj != null)
                {
                    cmd.CommandText = $"SELECT * FROM [{tableName}] WHERE {columnName}='{RemoveXSDType(obj)}'";
                }
                else
                {
                    cmd.CommandText = $"SELECT * FROM [{tableName}] WHERE {columnName} IS NOT NULL";
                }
                conn.Open();
                SqlDataReader reader = cmd.ExecuteReader();
                int counter = 0; //in case there's no ID field
                string dbName = GetDBName();
                string pk = GetPrimaryKeys(dbName, tableName)[0];
                while (reader.Read())
                {
                    counter++;
                    RawTriple triple = new RawTriple
                    {
                        Subj = $"{prefixURI}{dbName}/{tableName}/{pk}.{(reader[pk] ?? counter)}", // dbName + tableName + (reader["ID"] ?? ++counter),
                        Pred = $"{prefixURI}{dbName}/{tableName}#{columnName}",
                        Obj = obj ?? reader[columnName].ToString()
                    };
                    ////////////////////////
                    if(obj == null || !triple.Obj.Contains("^^xsd:")) //if object was not specified by user or if user didn't specify the dataType by himself
                    {
                        //attach a datatype
                        string xsdType = MappingGenerator.SqlToXsdDtMapper.MapSqlToXSD(GetColumnSqlDataType(tableName, columnName));
                        if (xsdType != "string")
                        {
                            triple.Obj += $"^^xsd:{xsdType}";
                        }
                    } 
                    triples.Add(triple);

                    if(IFPs!= null)
                    {
                        foreach(string columnURI in IFPs)
                        {
                            if (columnURI.Contains($"{prefixURI}{dbName}/{tableName}"))   //IFP columnURI should correspond to current entity, not the FEDERATED one, for example
                            {
                                string ifpColumnName = columnURI.Replace($"{prefixURI}{dbName}/{tableName}#", "");
                                RawTriple tripleIFP = new RawTriple
                                {
                                    Subj = $"{prefixURI}{dbName}/{tableName}/{pk}.{(reader[pk] ?? counter)}", // dbName + tableName + (reader["ID"] ?? ++counter),
                                    Pred = columnURI,
                                    Obj = obj ?? reader[ifpColumnName].ToString()
                                };
                                triples.Add(tripleIFP);
                            }
                        }
                    }

                    //triples.Add(new RawTriple { Subj = tableName + (reader["ID"] ?? ++counter), Pred = "Table", Obj = tableName });
                }
            }
            return triples;
        }

        private string GetColumnSqlDataType(string tableName, string columnName)
        {
            string dType = string.Empty;
            using (SqlConnection conn = new SqlConnection(connString))
            {
                conn.Open();
                DataTable schema = conn.GetSchema("Columns");

                dType = (from DataRow row in schema.Rows
                        where ((string)row[2]) == tableName &&
                              ((string)row[3]) == columnName
                        select (string)row[7]).First();

                return dType;
            }
        }

        private string RemoveXSDType(string xsdObjectString)
        {
            return xsdObjectString.Replace("xsd:", "");
        }

        public List<RawTriple> GetTriplesForSubject(string tableName, string individualColName, string individualColValue,
                                                    string prefixURI, string predicateColName = null, string obj=null)
        {
            List<RawTriple> triples = new List<RawTriple>();
            if (obj != null)
            {
                obj = obj.Trim('"');
            }
            using (SqlConnection conn = new SqlConnection(connString))
            {
                SqlCommand cmd = conn.CreateCommand();
                bool subjObj = obj != null;
                bool subjPred = predicateColName != null;
                bool subjPredObj = subjObj && subjPred;

                if(subjPredObj)
                {
                    throw new ArgumentException("Cannot query for <subject> <predicate> \"object\"!");
                }
                else if(subjObj)
                {
                    //<subject> ?predicate "Object"
                    /*
                        SELECT *
                        FROM table(subject)
                        WHERE User.ID=1
                        //check for "Object" inside DataReader, when queried 
                    */
                    cmd.CommandText = $"SELECT * FROM [{tableName}] WHERE {individualColName} = {individualColValue}";
                    conn.Open();
                    SqlDataReader reader = cmd.ExecuteReader();
                    string dbName = GetDBName();
                    string pk = GetPrimaryKeys(dbName, tableName)[0];
                    while (reader.Read())
                    {
                        for(int i=0; i<reader.FieldCount; i++)
                        {
                            if(reader[i].ToString()==obj)
                            {
                                RawTriple triple = new RawTriple
                                {
                                    Subj = $"{prefixURI}{dbName}/{tableName}/{individualColName}.{individualColValue}",
                                    Pred = $"{prefixURI}{dbName}/{tableName}#{reader.GetName(i)}",
                                    Obj = obj
                                };
                                triples.Add(triple);
                            } 


                        }
                    }
                }
                else if(subjPred)
                {
                    //<subject> <predicate> ?object
                    /*
                        SELECT User.NAME
                        FROM table(subject)
                        WHERE User.ID=1 AND User.NAME IS NOT NULL
                    */
                    cmd.CommandText = $"SELECT {predicateColName} FROM [{tableName}] WHERE {individualColName} = {individualColValue} AND {predicateColName} IS NOT NULL";
                    conn.Open();
                    SqlDataReader reader = cmd.ExecuteReader();
                    string dbName = GetDBName();
                    while (reader.Read())
                    {
                        RawTriple triple = new RawTriple
                        {
                            Subj = $"{prefixURI}{dbName}/{tableName}/{individualColName}.{individualColValue}",
                            Pred = $"{prefixURI}{dbName}/{tableName}#{predicateColName}",
                            Obj = reader[predicateColName].ToString()
                        };
                        triples.Add(triple);      
                    }
                }
                else if((subjObj || subjPred)== false) //only subject
                {
                    //<subject> ?predicate ?object
                    /*
                        SELECT *
                        FROM table(subject)
                        WHERE User.ID=1
                    */
                    cmd.CommandText = $"SELECT * FROM [{tableName}] WHERE {individualColName} = {individualColValue}";
                    conn.Open();
                    SqlDataReader reader = cmd.ExecuteReader();
                    string dbName = GetDBName();
                    while (reader.Read())
                    {
                        for(int i=0; i<reader.FieldCount; i++)
                        {
                            string objStr = reader[i].ToString();
                            if (string.IsNullOrEmpty(objStr))
                                continue;
                            RawTriple triple = new RawTriple
                            {
                                Subj = $"{prefixURI}{dbName}/{tableName}/{individualColName}.{individualColValue}",
                                Pred = $"{prefixURI}{dbName}/{tableName}#{reader.GetName(i)}",
                                Obj = objStr
                            };
                            triples.Add(triple);
                        }
                    }
                }
            }
            return triples;
        }

        public List<string> GetPrimaryKeys(string dbName, string tableName)
        {
            SqlConnection sqlConnection = new SqlConnection(connString);
            //build a "serverConnection" with the information of the "sqlConnection"
            ServerConnection serverConnection = new ServerConnection(sqlConnection);

            //The "serverConnection is used in the ctor of the Server.
            Server server = new Server(serverConnection);
            Database db = server.Databases[dbName];

            Table tbl = db.Tables[tableName];

            List<string> pkList = new List<string>();
            foreach (Column col in tbl.Columns)
            {
                if (col.InPrimaryKey)
                    pkList.Add(col.Name);
            }

            return pkList;
        }

        public string GetFKReferencingPKTable(string dbName, string pkTableName, string referencingTable)
        {
            SqlConnection sqlConnection = new SqlConnection(connString);
            //build a "serverConnection" with the information of the "sqlConnection"
            ServerConnection serverConnection = new ServerConnection(sqlConnection);

            //The "serverConnection is used in the ctor of the Server.
            Server server = new Server(serverConnection);
            Database db = server.Databases[dbName];

            Table tbl = db.Tables[referencingTable];

            foreach (ForeignKey fk in tbl.ForeignKeys)
            {
                if (fk.ReferencedTable.ToString() == pkTableName)
                    return fk.Columns[0].Name;
            }

            throw new KeyNotFoundException($"Foreign key referencing table '{referencingTable}' was not found in table '{pkTableName}'");
        }

        public string GetPKReferencedByFK(string dbName, string fkTableName, string fkName)
        {
            SqlConnection sqlConnection = new SqlConnection(connString);
            //build a "serverConnection" with the information of the "sqlConnection"
            ServerConnection serverConnection = new ServerConnection(sqlConnection);

            //The "serverConnection is used in the ctor of the Server.
            Server server = new Server(serverConnection);
            Database db = server.Databases[dbName];

            Table tbl = db.Tables[fkTableName];

            foreach (ForeignKey fk in tbl.ForeignKeys)
            {
                if (fk.Name == fkName)
                    return fk.ReferencedKey;
            }

            throw new KeyNotFoundException($"Primary key referenced by FK '{fkName}' in table '{fkTableName}' was not found!");
        }
    }
}