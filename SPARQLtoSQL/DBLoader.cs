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

    public class DBLoader
    {
        string connString;

        public DBLoader(string connString)
        {
            this.connString = connString;
        }

        public List<string> GetTableNames()
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

        public List<RawTriple> GetTriplesForPredicateObject(string tableName, string columnName, string prefixURI, string obj)
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
                    cmd.CommandText = $"SELECT * FROM [{tableName}] WHERE {columnName}='{obj}'";
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
                    RawTriple triple = new RawTriple
                    {
                        Subj = $"{prefixURI}{dbName}/{tableName}/{pk}.{(reader[pk] ?? ++counter)}", // dbName + tableName + (reader["ID"] ?? ++counter),
                        Pred = $"{prefixURI}{dbName}/{tableName}#{columnName}",
                        Obj = obj ?? reader[columnName].ToString()
                    };
                    triples.Add(triple);

                    //triples.Add(new RawTriple { Subj = tableName + (reader["ID"] ?? ++counter), Pred = "Table", Obj = tableName });
                }
            }
            return triples;
        }

        private List<string> GetPrimaryKeys(string dbName, string tableName)
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
    }
}