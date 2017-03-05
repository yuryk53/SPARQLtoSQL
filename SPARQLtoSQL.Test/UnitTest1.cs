using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using SPARQLtoSQL;
using VDS.RDF;
using VDS.RDF.Query.Inference;
using VDS.RDF.Query;
using VDS.RDF.Parsing;
using System.Collections.Generic;

namespace SPARQLtoSQL.Test
{
    [TestClass]
    public class SPARQLtoSQLTests
    {
        [TestMethod]
        public void TestSubjObjPatterns()
        {
            string queryStr = @"SELECT *
								   WHERE { ?usr <http://www.semanticweb.org/LMS/User#ROLE_ID> ?roleID.
                                           ?role <http://www.semanticweb.org/LMS/Role#ID> ?roleID.
                                           ?role <http://www.semanticweb.org/LMS/Role#NAME> ""Teacher""}";
            TripleStore store = null;
            SparqlQuery query = null;
            Arrange(queryStr, out store, out query);

            //ACT
            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);//new LeviathanQueryProcessor(store);   //process query
            var results = processor.ProcessQuery(query) as SparqlResultSet;

            //ASSERT
            Assert.IsInstanceOfType(results, typeof(SparqlResultSet));

            if (results is SparqlResultSet)
            {
                SparqlResultSet rset = (SparqlResultSet)results;
                Assert.IsTrue(rset.Count == 3);
                foreach(var result in results)
                {
                    Assert.IsTrue(result.Value("roleID").ToString() == "2");
                }
            }
        }

        [TestMethod]
        public void TestSubjPatternObjLiteral()
        {
            string queryStr = @"SELECT *
                                 WHERE { ?usr <http://www.semanticweb.org/LMS/User#NAME> ""Alexander Cole""}";
            SparqlQuery query = null;
            TripleStore store = null;
            Arrange(queryStr, out store, out query);

            //ACT
            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);//new LeviathanQueryProcessor(store);   //process query
            var results = processor.ProcessQuery(query) as SparqlResultSet;

            //ASSERT
            Assert.IsInstanceOfType(results, typeof(SparqlResultSet));

            if (results is SparqlResultSet)
            {
                SparqlResultSet rset = (SparqlResultSet)results;
                Assert.IsTrue(rset.Count == 1);
            }
        }

        public void Arrange(string sparqlQuery, out TripleStore store, out SparqlQuery query)
        {
            IGraph g = new VDS.RDF.Graph();                 //Load triples from file OWL
            g.LoadFromFile(@"D:\Stud\4 course\II semester\!Diploma work\Software\Code\SPARQLtoSQL\SPARQLtoSQL\bin\Debug\combined.owl");
            g.BaseUri = null; //!

            RdfsReasoner reasoner = new RdfsReasoner(); //Apply reasoner
            reasoner.Initialise(g);
            reasoner.Apply(g);

            g.BaseUri = null;   //!!!!!!!!
            store = new TripleStore();
            store.Add(g);

            SparqlParameterizedString queryString = new SparqlParameterizedString();

            queryString.CommandText = sparqlQuery;

            SparqlQueryParser parser = new SparqlQueryParser();
            //SparqlQuery query = parser.ParseFromString(queryString.ToString());
            query = parser.ParseFromString(queryString.ToString());

            Dictionary<string, string> dbURIs = new Dictionary<string, string>();
            dbURIs.Add("http://www.semanticweb.org/KMS/", @"Data Source = ASUS\SQLEXPRESS; Initial Catalog = KMS; Integrated Security = True");
            dbURIs.Add("http://www.semanticweb.org/LMS/", @"Data Source = ASUS\SQLEXPRESS; Initial Catalog = LMS; Integrated Security = True");
            Program.ResolveBGPsFromDB(query.ToAlgebra(), g, dbURIs);

            Console.WriteLine(query.ToAlgebra());
            Console.WriteLine(query.ToString());   
        }
    }
}
