using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPARQLtoSQL
{
    public interface IDBLoader
    {
        string ConnectionString { get; set; }

        /// <summary>
        /// Gets the table names of the database, represented by ConnectionString
        /// </summary>
        /// <returns>Table names</returns>
        List<string> GetTableNames();
        /// <summary>
        /// Gets the column names of the particular table in the DB
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns>List of column names</returns>
        List<string> GetColumnNames(string tableName);
        /// <summary>
        /// Gets the name of the current database
        /// </summary>
        /// <returns>Current database name, which is represented with ConnectionString</returns>
        string GetDBName();

        #region SemanticData

        /// <summary>
        /// Gets all the triples from table in format ?subject ?predicate ?object
        /// </summary>
        /// <param name="tableName">Table name</param>
        /// <param name="tableColumns">Table columns, which are included in the query</param>
        /// <returns>The list of raw triples, queried from the database</returns>
        List<RawTriple> GetTriplesFromTable(string tableName, params string[] tableColumns);
        /// <summary>
        /// Given predicate and object 
        /// </summary>
        /// <param name="tableName">The table name where from to generate triples</param>
        /// <param name="columnName">The predicate value in triple pattern=table name</param>
        /// <param name="prefixURI">Prefix URI to include in triples</param>
        /// <param name="obj">Object value in triple pattern</param>
        /// <returns>List of triple patterns for ?subject predicate object pattern</returns>
        List<RawTriple> GetTriplesForPredicateObject(string tableName, string columnName, string prefixURI, string obj);
        /// <summary>
        /// Given the subject in triple pattern, gets the triples that correspond to it
        /// </summary>
        /// <param name="tableName">The table name where from to generate triples</param>
        /// <param name="individualColName">The column name == predicate</param>
        /// <param name="individualColValue">The individual record column value == object</param>
        /// <param name="prefixURI">Prefix URI to include in resulting triples</param>
        /// <param name="predicateColName">The column name = predicate. Is provided only if triples are to be gotten for particular column.</param>
        /// <param name="obj">The object value in tripple pattern. If NULL, then it's treated as ?object pattern</param>
        /// <returns>List of triples for triple pattern SUBJECT ?predicate ?object </returns>
        List<RawTriple> GetTriplesForSubject(string tableName, string individualColName, string individualColValue,
                                                    string prefixURI, string predicateColName = null, string obj = null);
        #endregion

        /// <summary>
        /// Gets the primary keys of some DB table.
        /// </summary>
        /// <param name="dbName">Database name</param>
        /// <param name="tableName">Table name</param>
        /// <returns>The list of primary keys of table.</returns>
        List<string> GetPrimaryKeys(string dbName, string tableName);
    }
}
