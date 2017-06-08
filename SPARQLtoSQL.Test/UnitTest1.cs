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
        public SPARQLtoSQLTests()
        {
            DBLoaderFactory.RegisterDBLoaders(typeof(MSSQLdBLoader),
                @"Data Source = ASUS\SQLEXPRESS; Initial Catalog = KMS; Integrated Security = True",
                @"Data Source = ASUS\SQLEXPRESS; Initial Catalog = LMS; Integrated Security = True");
        }

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
                    Console.WriteLine(result.ToString());
                }
            }
        }

        [TestMethod]
        public void TestFEDERATED_SubjObjPattern_PredURI()
        {
            string queryStr = @"SELECT *
								        WHERE { ?kunde <http://www.semanticweb.org/FEDERATED/Kunde#NAME> ?name }";
            TripleStore store = null;
            SparqlQuery query = null;
            Arrange(queryStr, out store, out query);

            //ACT
            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);
            var results = processor.ProcessQuery(query) as SparqlResultSet;

            //ASSERT
            Assert.IsInstanceOfType(results, typeof(SparqlResultSet));

            if (results is SparqlResultSet)
            {
                SparqlResultSet rset = (SparqlResultSet)results;
                Assert.IsTrue(rset.Count == 20);
                foreach (var result in results)
                {
                    Console.WriteLine(result.ToString());
                }
            }
        }

        [TestMethod]
        public void TestFEDERATED_SubjPattern_PredURI_ObjLiteral()
        {
            string queryStr = @"SELECT *
								WHERE { ?kunde <http://www.semanticweb.org/FEDERATED/Kunde#NAME> ""Alexander Cole"" }";
            TripleStore store = null;
            SparqlQuery query = null;
            Arrange(queryStr, out store, out query);

            //ACT
            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);
            var results = processor.ProcessQuery(query) as SparqlResultSet;

            //ASSERT
            Assert.IsInstanceOfType(results, typeof(SparqlResultSet));

            if (results is SparqlResultSet)
            {
                SparqlResultSet rset = (SparqlResultSet)results;
                Assert.IsTrue(rset.Count == 1);
                foreach (var result in results)
                {
                    Console.WriteLine(result.ToString());
                }
            }
        }

        [TestMethod]
        public void TestFEDERATED_SubjPattern_PredURI_ObjPattern_Filter()
        {
            string queryStr = @"SELECT *
								        WHERE { ?kunde <http://www.semanticweb.org/FEDERATED/Kunde#NAME> ?name.
                                                filter regex(str(?name), '^Alexander')}";
            TripleStore store = null;
            SparqlQuery query = null;
            Arrange(queryStr, out store, out query);

            //ACT
            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);
            var results = processor.ProcessQuery(query) as SparqlResultSet;

            //ASSERT
            Assert.IsInstanceOfType(results, typeof(SparqlResultSet));

            if (results is SparqlResultSet)
            {
                SparqlResultSet rset = (SparqlResultSet)results;
                Assert.IsTrue(rset.Count == 2);
                foreach (var result in results)
                {
                    Console.WriteLine(result.ToString());
                }
            }
        }

        [TestMethod]
        public void TestQueryBothKMS_LMS()
        {
            string queryStr = @"SELECT *
                                    WHERE { ?usr <http://www.semanticweb.org/LMS/User#EMAIL> ?email.
                                            ?usr1 <http://www.semanticweb.org/KMS/User#EMAIL> ?email}";
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
                foreach (var result in results)
                {
                    Console.WriteLine(result.ToString());
                }
                Assert.IsTrue(rset.Count == 7);
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

        [TestMethod]
        public void TestSubjPredURIs()
        {
            string queryStr = @"SELECT *
								WHERE { <http://www.semanticweb.org/LMS/User/ID.1> <http://www.semanticweb.org/LMS/User#ROLE_ID> ?roleID}";
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
                Assert.IsTrue(rset.Count == 1);
                foreach (var result in results)
                {
                    Assert.IsTrue(result.Value("roleID").ToString() == "1");
                    Console.WriteLine(result.ToString());
                }
            }
        }

        [TestMethod]
        public void TestSubjURI_ObjLiteral()
        {
            string queryStr = @"SELECT *
								WHERE { <http://www.semanticweb.org/LMS/User/ID.1> ?predicate ""Alexander Cole""}";
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
                Console.WriteLine($"Count: {rset.Count}");
                Assert.IsTrue(rset.Count == 1);
                foreach (var result in results)
                {
                    Console.WriteLine(result.ToString());
                    Assert.IsTrue(result.Value("predicate").ToString() == "http://www.semanticweb.org/LMS/User#NAME");
                    
                }
            }
        }

        [TestMethod]
        public void TestSubjURI_PredObjPatterns()
        {
            string queryStr = @"SELECT *
								WHERE { <http://www.semanticweb.org/LMS/User/ID.1> ?predicate ?obj}";
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
                Console.WriteLine($"Count: {rset.Count}");
                Assert.IsTrue(rset.Count == 5);

                foreach (var result in results) //should output all attributes and their values
                {
                    Console.WriteLine(result.ToString());
                }
            }
        }

        [TestMethod]
        public void TestSubjPredObjURIs_EXCEPTION()
        {
            //INCORRECT QUERY
            string queryStr = @"SELECT *
								WHERE { <http://www.semanticweb.org/LMS/User/ID.1> <http://www.semanticweb.org/LMS/User#ROLE> <http://www.semanticweb.org/LMS/Role/ID.1>}"; //INCORRECT QUERY!!!
            TripleStore store = null;
            SparqlQuery query = null;

            //ACT
            try {
                Arrange(queryStr, out store, out query);
            }catch (Exception ex)
            {
                //ASSERT
                Assert.IsInstanceOfType(ex, typeof(ArgumentException));
            }
        }

        [TestMethod]
        public void TestComplex_KMS_LMS_query()
        {
            //INCORRECT QUERY
            string queryStr = @"SELECT ?user ?doc_name
								        WHERE { ?user <http://www.semanticweb.org/LMS/User#EMAIL> ?email.
                                                ?user1 <http://www.semanticweb.org/KMS/User#EMAIL> ?email.
                                                ?user1 <http://www.semanticweb.org/KMS/User#ID> ?user1_id.
                                                ?doc_id <http://www.semanticweb.org/KMS/Document#AUTHOR_ID> ?user1_id.
                                                ?doc_id <http://www.semanticweb.org/KMS/Document#NAME> ?doc_name}";
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
                Console.WriteLine($"Count: {rset.Count}");

                foreach (var result in results) //should output all attributes and their values
                {
                    Console.WriteLine(result.ToString());
                }
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
            //Program.ResolveBGPsFromDB(query.ToAlgebra(), g, dbURIs, new SPARQLtoSQL.DBLoaderFactory(), new List<Triple>(), new Dictionary<string, List<string>>());

            List<Triple> triplesToAdd = new List<Triple>();
            Program.ResolveBGPsFromDB(query.ToAlgebra(), g, dbURIs, new DBLoaderFactory(), triplesToAdd);
            //reasoner.Initialise(g);
            //reasoner.Apply(g);
            Program.MatchRecordsIFPAndStore(g, triplesToAdd);

            Console.WriteLine(query.ToAlgebra());
            Console.WriteLine(query.ToString());   
        }
    }
}
