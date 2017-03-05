using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;

using System.Text.RegularExpressions;

using VDS.RDF;
using VDS.RDF.Writing;
using VDS.RDF.Query;
using VDS.RDF.Parsing;
using VDS.RDF.Query.Inference;
using VDS.RDF.Storage;
using VDS.RDF.Query.Algebra;
using VDS.RDF.Query.Patterns;

namespace SPARQLtoSQL
{
    public class Program
    {
        static Mapping mapping = new Mapping();
        static Dictionary<Node, string> nodesToSqlMap = new Dictionary<Node, string>();

        static void Main(string[] args)
        {
            mapping.ReadMappings("mapping.tmap");


            //ProjectNode p = new ProjectNode(null, new List<string>(new string[] { "x" }));
            //JoinNode j = new JoinNode(parent: p);
            //TerminalNode t1 = new TerminalNode(parent: j, subj: "?x", pred: "rdf:type", obj: ":Neoplasm");
            //TerminalNode t2 = new TerminalNode(parent: j, subj: "?x", pred: ":hasStage", obj: ":stage-IIIa");
            //SparqlGraph g = new SparqlGraph();
            //g.Root = p;

            //List<Node> sorted = g.TopologicalSort();
            //PopulateNodesToSqlMap(g); //TODO: T-mappings derivation (from user once) should be run first!!!! (Phase 2)

            //Console.WriteLine(mappings[":hasName"][0].IsMatch("?x", ":hasName", ":Neoplasm"));
            
            CustomQueryLMS_KMS();
            //string connString = @"Data Source=ASUS\SQLEXPRESS;Initial Catalog=KMS;Integrated Security=True";
        }

        static string GetConnStringFromURI(Dictionary<string, string> dbURIs, string uri)
        {
            uri = uri.Trim('<', '>');
            uri = uri.Replace(" ", "");

            string dbURIsKey = dbURIs.Keys.FirstOrDefault(key => (key.Length <= uri.Length) ? key == uri.Substring(0, key.Length) : false);
            if (dbURIsKey != null)
            {
                return dbURIs[dbURIsKey];
            }
            else return null;
        }

        static bool IsLiteralValue(string value)
        {
            value = value.TrimStart().TrimEnd();
            bool hasQuotes = (value[0] == '"' && value[value.Length - 1] == '"');
            bool hasAngleBracket = (value[0] == '<' && value[value.Length - 1] == '>');
            return hasQuotes || !hasAngleBracket;
        }

        public static void ResolveBGPsFromDB(ISparqlAlgebra algebra, IGraph g, Dictionary<string,string> dbURIs)
        {
            if(algebra is IBgp)
            {
                IBgp bgp = algebra as IBgp;
                //do work here
                /*
                    resolve DB name from subj/predicate/obj
                    resolve Table name
                    make query
                    convert results to rdf triples
                    add all the triples to IGraph g
                */
                var triples = bgp.TriplePatterns;

                foreach(TriplePattern triple in triples)
                {
                    //do work here for each triple

                    string subjConnString = GetConnStringFromURI(dbURIs, triple.Subject.ToString());
                    string predConnString = GetConnStringFromURI(dbURIs, triple.Predicate.ToString());
                    string objConnString = GetConnStringFromURI(dbURIs, triple.Object.ToString());

                    if(subjConnString==null && predConnString==null && objConnString==null)
                    {
                        //we deal with request to FEDERATED schema or it's an error
                        //if it's FEDERATED schema, we should find subclasses/subproperties, equivalent classes/properties and query for them

                        if(triple.Subject.VariableName == null) //is not a pattern
                        {
                            //IN FEDERATED schema there're no individuals, so we can't have subject URI or subject literal here!

                            //subject here could not be a URI! it would be logically incorrect!!!!!
                            //it could only be a pattern

                            throw new InvalidOperationException("Subject variable in tripple, referring to FEDERATED schema should be a PATTERN!");
                            //throw new NotImplementedException();
                        }
                        if (triple.Predicate.VariableName == null) //is not a pattern
                        {
                            //query for equivalent properties
                            TripleStore store = new TripleStore();
                            store.Add(g);

                            SparqlParameterizedString queryString = new SparqlParameterizedString();

                            queryString.Namespaces.AddNamespace("owl", new Uri("http://www.w3.org/2002/07/owl#"));
                            queryString.CommandText = @"SELECT ?property1 WHERE {
                                                        ?property owl:equivalentProperty ?property1
                                                        filter regex(str(?property), '^"+triple.Predicate.ToString().Trim('<','>')+"')}";

                            SparqlQueryParser parser = new SparqlQueryParser();
                            SparqlQuery query = parser.ParseFromString(queryString.ToString());

                            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);   //process query
                            var results = processor.ProcessQuery(query) as SparqlResultSet;
                            Console.WriteLine();

                            //object can be a literal or a pattern
                            foreach (SparqlResult resultPredicate in results)
                            {
                                //query with new predicates and transform the results to FEDERATED schema syntax
                                queryString = new SparqlParameterizedString();
                                queryString.CommandText = $"SELECT * WHERE {{ ?subj <{resultPredicate[0].ToString()}> {triple.Object.ToString()} }} ";
                                SparqlResultSet resultSet = QuerySparqlFromDB(g, queryString, dbURIs);

                                string federatedStem = triple.Predicate.ToString().Trim('<', '>').Split('#')[0]; //left part (before '#') -> /FEDERATED/table name
                                //federatedStem += '/'; // /FEDERATED/table name/

                                foreach (SparqlResult result in resultSet)
                                {
                                    Dictionary<string, string> dbInfo = GetDatabaseInfoForIndividualURI(result[0].ToString());
                                    string subjStr = $"{federatedStem}/{dbInfo["dbName"]}.{dbInfo["columnName"]}.{dbInfo["columnValue"]}";
                                    string predStr = triple.Predicate.ToString().Trim('<', '>');
                                    string objStr;
                                    if (triple.Object.VariableName == null) //not a pattern
                                    {
                                        objStr = triple.Object.ToString().Trim('"');
                                    }
                                    else //object was a pattern ?object in sparql query
                                    {
                                        //dbInfo = GetDatabaseInfoForIndividualURI(result[1].ToString());
                                        if(IsLiteralValue(result[1].ToString()))
                                        {
                                            objStr = result[1].ToString();
                                        }
                                        else
                                        {
                                            dbInfo = GetDatabaseInfoForIndividualURI(result[1].ToString());
                                            objStr = $"{federatedStem}/{dbInfo["dbName"]}.{dbInfo["columnName"]}.{dbInfo["columnValue"]}";
                                        } 
                                    }
                                    INode subj = g.CreateUriNode(new Uri(subjStr));
                                    INode pred = g.CreateUriNode(new Uri(predStr));
                                    INode obj; // = g.CreateLiteralNode($"{rawTriple.Obj}");
                                    if (IsLiteralValue(objStr))
                                    {
                                        obj = g.CreateLiteralNode(objStr);
                                    }
                                    else obj = g.CreateUriNode(new Uri(objStr));
                                    g.Assert(new Triple(subj, pred, obj));
                                }
                            }


                            //throw new NotImplementedException();

                        }
                        if(triple.Object.VariableName == null) //is not a pattern
                        {
                            if(!IsLiteralValue(triple.Object.ToString()))
                            {
                                throw new InvalidOperationException("Object variable in tripple, referring to FEDERATED schema should be a PATTERN!");
                                //throw new NotImplementedException();
                            }
                            
                        }
                        //throw new NotImplementedException();
                    }

                    if(subjConnString != null) //most probably, subjectConnString will be NULL (cause subject may be a pattern)
                    {
                        //TODO
                        //if subj is not a literal, we should query DB for subj triples!!!
                        //<http://www.semanticweb.org/LMS/User/ID.1> <http://www.semanticweb.org/LMS/User#NAME> ?name

                        //<subject> <predicate> ?object
                        /*
                            SELECT User.NAME
                            FROM table(subject)
                            WHERE User.ID=1 AND User.NAME IS NOT NULL
                        */

                        //<subject> ?predicate ?object
                        /*
                            SELECT *
                            FROM table(subject)
                            WHERE User.ID=1
                        */

                        //<subject> ?predicate "Object"
                        /*
                            SELECT *
                            FROM table(subject)
                            WHERE User.ID=1
                            //check for "Object" inside DataReader, when queried 
                        */

                        DBLoader dbLoader = new DBLoader(subjConnString);
                        Dictionary<string, string> dbInfo = GetDatabaseInfoForIndividualURI(triple.Subject.ToString());
                        
                        List<RawTriple> rawTriples = dbLoader.GetTriplesForSubject(
                            tableName: dbInfo["tableName"],
                            individualColName: dbInfo["columnName"],
                            individualColValue:dbInfo["columnValue"],
                            prefixURI: dbInfo["prefix"],
                            predicateColName: triple.Predicate.VariableName==null ? GetPrefixDbNameTableNameColNameFromURI(triple.Predicate.ToString())["columnName"] : null,
                            obj: triple.Object.VariableName==null ? triple.Object.ToString().Trim('>', '<') : null
                        );
                        foreach (var rawTriple in rawTriples)
                        {
                            INode subj = g.CreateUriNode(new Uri(rawTriple.Subj));
                            INode pred = g.CreateUriNode(new Uri(rawTriple.Pred));
                            INode obj = g.CreateLiteralNode($"{rawTriple.Obj}");
                            g.Assert(new Triple(subj, pred, obj));
                        }
                    }
                    if(predConnString != null)
                    {
                        //referring to DataType- or Object- Property
                        if(triple.Object.VariableName == null) //object is not a pattern
                        {
                            if (IsLiteralValue(triple.Object.ToString()))
                            {
                                // ?s <predicate> "Object"

                                /*
                                    SELECT *
                                    FROM table(predicate)
                                    WHERE table.Attribute="Object"
                                */
                                DBLoader dbLoader = new DBLoader(predConnString);
                                Dictionary<string, string> dbInfo = GetPrefixDbNameTableNameColNameFromURI(triple.Predicate.ToString());
                                List<RawTriple> rawTriples = dbLoader.GetTriplesForPredicateObject(
                                    tableName: dbInfo["tableName"],
                                    columnName: dbInfo["columnName"],
                                    prefixURI: dbInfo["prefix"],
                                    obj: triple.Object.ToString());
                                foreach (var rawTriple in rawTriples)
                                {
                                    INode subj = g.CreateUriNode(new Uri(rawTriple.Subj));
                                    INode pred = g.CreateUriNode(new Uri(rawTriple.Pred));
                                    INode obj = g.CreateLiteralNode($"{rawTriple.Obj}");
                                    g.Assert(new Triple(subj, pred, obj));
                                }
                            }
                            else if(objConnString!= null)
                            {
                                // ?s <predicate> <object>
                                throw new NotImplementedException();
                            }
                        }
                        else //object is a pattern
                        {
                            //?s <predicate> ?object

                            /*
                                SELECT *
                                FROM table(predicate)
                                WHERE table.Attribute="Object"
                            */
                            DBLoader dbLoader = new DBLoader(predConnString);
                            Dictionary<string, string> dbInfo = GetPrefixDbNameTableNameColNameFromURI(triple.Predicate.ToString());
                            List<RawTriple> rawTriples = dbLoader.GetTriplesForPredicateObject(
                                tableName: dbInfo["tableName"],
                                columnName: dbInfo["columnName"],
                                prefixURI: dbInfo["prefix"],
                                obj: null);
                            foreach (var rawTriple in rawTriples)
                            {
                                INode subj = g.CreateUriNode(new Uri(rawTriple.Subj));
                                INode pred = g.CreateUriNode(new Uri(rawTriple.Pred));
                                INode obj = g.CreateLiteralNode($"{rawTriple.Obj}");
                                g.Assert(new Triple(subj, pred, obj));
                            }
                        }

                    }
                    if(objConnString != null) //object is not a pattern
                    {
                        //TODO
                        throw new NotImplementedException();
                    }

                    //check if subj, pred and obj refer to one DB

                }
            }
            else
            {
                if(algebra is IUnaryOperator)
                {
                    algebra = algebra as IUnaryOperator;
                    ResolveBGPsFromDB((algebra as IUnaryOperator).InnerAlgebra, g, dbURIs);
                }
                else if(algebra is IAbstractJoin)
                {
                    ResolveBGPsFromDB((algebra as IAbstractJoin).Lhs, g, dbURIs);
                    ResolveBGPsFromDB((algebra as IAbstractJoin).Rhs, g, dbURIs);
                }
            }
        }

        static SparqlResultSet QuerySparqlFromDB(IGraph g, SparqlParameterizedString sparqlQuery, Dictionary<string,string> dbURIs)
        {
            TripleStore store = new TripleStore();
            store.Add(g);

            SparqlQueryParser parser = new SparqlQueryParser();
            SparqlQuery query = parser.ParseFromString(sparqlQuery.ToString());

            ResolveBGPsFromDB(query.ToAlgebra(), g, dbURIs);

            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);   //process query
            var results = processor.ProcessQuery(query) as SparqlResultSet;
            return results;
        }

        static Dictionary<string,string> GetPrefixDbNameTableNameColNameFromURI(string uri)
        {
            uri = uri.Trim('<', '>');
            Dictionary<string, string> result = new Dictionary<string, string>();
            Regex r = new Regex(@"(http\w{0,1}://.+/)(\w+)/(\w+)#(\w+)");
            if(r.IsMatch(uri))
            {
                Match match = r.Match(uri);
                result["prefix"] = match.Groups[1].Value;
                result["dbName"] = match.Groups[2].Value;
                result["tableName"] = match.Groups[3].Value;
                result["columnName"] = match.Groups[4].Value;
                return result;
            }
            throw new ArgumentException($"URI string {uri} is not a corrent URI!");
        }

        static Dictionary<string,string> GetDatabaseInfoForIndividualURI(string individualURI)
        {
            individualURI = individualURI.Trim('<', '>');
            Dictionary<string, string> result = new Dictionary<string, string>();
            Regex r = new Regex(@"(http\w{0,1}://.+/)(\w+)/(\w+)/(\w+).(.+)");
            if (r.IsMatch(individualURI))
            {
                Match match = r.Match(individualURI);
                result["prefix"] = match.Groups[1].Value;
                result["dbName"] = match.Groups[2].Value;
                result["tableName"] = match.Groups[3].Value;
                result["columnName"] = match.Groups[4].Value;
                result["columnValue"] = match.Groups[5].Value;
                return result;
            }
            throw new ArgumentException($"URI string {individualURI} is not a correct individual URI!");
        }

        static void CustomQueryLMS_KMS()
        {
            IGraph g = new VDS.RDF.Graph();                 //Load triples from file OWL
            g.LoadFromFile("combined.owl");
            g.BaseUri = null; //!

            //INode subj = g.CreateUriNode(new Uri("http://www.semanticweb.org/KMS/User/1"));
            //INode pred = g.CreateUriNode(new Uri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"));
            //INode obj = g.CreateUriNode(new Uri("http://www.semanticweb.org/KMS/User"));
            //g.Assert(new Triple(subj,pred,obj));
            

            RdfsReasoner reasoner = new RdfsReasoner(); //Apply reasoner
            reasoner.Initialise(g);
            reasoner.Apply(g);

            

            g.BaseUri = null;   //!!!!!!!!
            TripleStore store = new TripleStore();
            store.Add(g);

            SparqlParameterizedString queryString = new SparqlParameterizedString();
            //queryString.CommandText = @"SELECT * WHERE { ?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.semanticweb.org/KMS/User>.
            //                                             OPTIONAL {?user <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.semanticweb.org/LMS/User>}}";
            // queryString.CommandText = @"SELECT *
            //WHERE { ?usr <http://www.semanticweb.org/LMS/User#EMAIL> ?email.
            //                                ?usr1 <http://www.semanticweb.org/KMS/User#EMAIL> ?email}";
            //queryString.CommandText = @"SELECT *
            //WHERE { ?usr <http://www.semanticweb.org/LMS/User#ROLE_ID> ?roleID.
            //                                ?role <http://www.semanticweb.org/LMS/Role#ID> ?roleID.
            //                                ?role <http://www.semanticweb.org/LMS/Role#NAME> ""Teacher""}";

            //queryString.Namespaces.AddNamespace("owl", new Uri("http://www.w3.org/2002/07/owl#"));
            //queryString.CommandText = @"SELECT * WHERE {
            //                                            ?property owl:equivalentProperty ?property1
            //                                            filter regex(str(?property), '^http://www.semanticweb.org/FEDERATED/Kunde#NAME$')}";

            queryString.CommandText = @"SELECT *
								        WHERE { ?s <http://www.semanticweb.org/FEDERATED/Kunde#EMAIL> ?email.
                                                ?s1 <http://www.semanticweb.org/LMS/User#EMAIL> ?email}";

            SparqlQueryParser parser = new SparqlQueryParser();
            //SparqlQuery query = parser.ParseFromString(queryString.ToString());
            SparqlQuery query = parser.ParseFromString(queryString.ToString());

            Dictionary<string, string> dbURIs = new Dictionary<string, string>();
            dbURIs.Add("http://www.semanticweb.org/KMS/", @"Data Source = ASUS\SQLEXPRESS; Initial Catalog = KMS; Integrated Security = True");
            dbURIs.Add("http://www.semanticweb.org/LMS/", @"Data Source = ASUS\SQLEXPRESS; Initial Catalog = LMS; Integrated Security = True");
            ResolveBGPsFromDB(query.ToAlgebra(), g, dbURIs);

            Console.WriteLine(query.ToAlgebra());
            Console.WriteLine(query.ToString());

            //ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);   //process query
            //SparqlResultSet results = processor.ProcessQuery(query) as SparqlResultSet;

            ISparqlQueryProcessor processor = new QuantumQueryProcessor(store);//new LeviathanQueryProcessor(store);   //process query
            var results = processor.ProcessQuery(query) as SparqlResultSet;

            if (results is SparqlResultSet)
            {
                SparqlResultSet rset = (SparqlResultSet)results;
                foreach (SparqlResult result in rset)
                {
                    Console.WriteLine(result);
                }
            }
        }


        static void CustomQuery()
        {
            Uri prefixRDF = new Uri("http://www.w3.org/1999/02/22-rdf-syntax-ns#");
            Uri prefixFilm = new Uri("http://www.semprog.com/film#");

            //IGraph g = new Graph();                 //Load triples from file OWL
            //g.LoadFromFile("film-ontology.owl");
            //g.BaseUri = null; //!
            IGraph g = new VDS.RDF.Graph();         //Load triples from server (OWL)

            RdfsReasoner reasoner = new RdfsReasoner(); //Apply reasoner
            reasoner.Initialise(g);
            reasoner.Apply(g);


            g.BaseUri = null;   //!!!!!!!!
            TripleStore store = new TripleStore();
            store.Add(g);

            SparqlParameterizedString queryString = new SparqlParameterizedString();
            queryString.Namespaces.AddNamespace("rdf", prefixRDF);
            queryString.Namespaces.AddNamespace("film", prefixFilm);
            queryString.CommandText = "SELECT ?who WHERE { ?who rdf:type film:Person }";

            string sparql = "";

            sparql = File.ReadAllText("sparql1.txt");

            SparqlQueryParser parser = new SparqlQueryParser();
            //SparqlQuery query = parser.ParseFromString(queryString.ToString());
            SparqlQuery query = parser.ParseFromString(sparql);

            Console.WriteLine(query.ToAlgebra());
            Console.WriteLine(query.ToString());

            //ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);   //process query
            //SparqlResultSet results = processor.ProcessQuery(query) as SparqlResultSet;

            ISparqlQueryProcessor processor = new MirageQueryProcessor(mapping);//new LeviathanQueryProcessor(store);   //process query
            string results = processor.ProcessQuery(query) as string;
            Console.WriteLine(results);
            //if (results is SparqlResultSet)
            //{
            //    SparqlResultSet rset = (SparqlResultSet)results;
            //    foreach (SparqlResult result in rset)
            //    {
            //        Console.WriteLine(result);
            //    }
            //}
        }

        static void PopulateNodesToSqlMap(SparqlGraph g)
        {
            List<Node> terminalNodes = g.GetAllNodes(typeof(TerminalNode));

            foreach(TerminalNode node in terminalNodes)
            {
                //try to match each node to sql expressions from mapping
                string key = $"{node.Pred} {node.Obj}";
                if (mapping.mappings.ContainsKey(key))
                {
                    string sql = mapping.mappings[key][0].SQL;
                    nodesToSqlMap.Add(node, sql);
                }
                else
                {
                    key = $"{node.Pred}";
                    if(mapping.mappings.ContainsKey(key))
                    {
                        string sql = mapping.mappings[key].Find(mapNode => mapNode.Object == node.Obj).SQL;
                        nodesToSqlMap.Add(node, sql);
                    }
                    else //there is no mapping for this node
                    {
                        throw new Exception($"No mapping for node {node.Subj} {node.Pred} {node.Obj} was provided!");
                    }
                }
            }
        }

        static string TranslateToSql(SparqlGraph g, Dictionary<string, List<MapNode>> map)
        {
            /*S = List of nodes in Q in a bottom-up topological order
              sqlM = a map from nodes to SQL expressions*/
            List<Node> S = g.TopologicalSort();
            
            foreach (Node n in S)
            {
                if(n is TerminalNode) //translating leaves
                {
                    //skip (this step was done in PopulateNodesToSqlMap)
                    continue;
                }
                else
                {
                    if(n is JoinNode)
                    {
                        Node n1 = n.Children[0];
                        Node n2 = n.Children[1];
                        nodesToSqlMap[n] = InnerJoin(nodesToSqlMap[n1], nodesToSqlMap[n2]);   
                    }
                    else if(n is OptionalNode)
                    {
                        Node n1 = n.Children[0];
                        Node n2 = n.Parent;
                        string e = (n as OptionalNode).JoinCondition;
                        nodesToSqlMap[n] = LeftJoin(nodesToSqlMap[n1], nodesToSqlMap[n2], e);
                    }
                    else if(n is UnionNode)
                    {
                        Node n1 = n.Children[0];
                        Node n2 = n.Children[1];
                        nodesToSqlMap[n] = Union(nodesToSqlMap[n1], nodesToSqlMap[n2]);
                    }
                    else if(n is FilterNode)
                    {
                        Node n1 = n.Children[0];
                        string e = (n as FilterNode).FilterExpression;
                        nodesToSqlMap[n] = Filter(nodesToSqlMap[n1], e);
                    }
                    else if(n is ProjectNode)
                    {
                        Node n1 = n.Children[0];
                        List<string> pv = (n as ProjectNode).ProjectionVariables;
                        nodesToSqlMap[n] = Project(nodesToSqlMap[n1], pv);
                    }
                }
            }
            return nodesToSqlMap[S.Last()];
        }

        static string InnerJoin(string q1, string q2)
        {
            throw new NotImplementedException();
        }

        static string LeftJoin(string q1, string q2, string e)
        {
            throw new NotImplementedException();
        }

        static string Union(string q1, string q2)
        {
            throw new NotImplementedException();
        }

        static string Filter(string q, string e)
        {
            throw new NotImplementedException();
        }

        static string Project(string q, List<string> pv)
        {
            throw new NotImplementedException();
        }

        //static void ReadMappings(string fName)
        //{
        //    using (StreamReader sr = new StreamReader(new FileStream(fName, FileMode.Open)))
        //    {
        //        while (!sr.EndOfStream)
        //        {
        //            //process node by node
        //            string[] nodeLine = sr.ReadLine().Split();
        //            //read SQL lines
        //            string sql = "";
        //            while (sr.Peek() == '\t')
        //            {
        //                sql += " " + sr.ReadLine().Remove(0,1);
        //            }
        //            sql = sql.Trim(new char[] { ' ', '.' });

        //            MapNode node = new MapNode
        //            {
        //                DBString = nodeLine[0],
        //                Predicate = nodeLine[1],
        //                Object = nodeLine[2],
        //                SQL = sql
        //            };

        //            Regex r = new Regex(@"\{.+\}");

                    
        //            string key = "";
        //            if(r.IsMatch(nodeLine[1]))
        //            {
        //                key = nodeLine[2];
        //            }
        //            else if(r.IsMatch(nodeLine[2]))
        //            {
        //                key = nodeLine[1];
        //            }
        //            else
        //                key = $"{nodeLine[1]} {nodeLine[2]}";

        //            if (mappings.ContainsKey(key))
        //            {
        //                mappings[key].Add(node);
        //            }
        //            else
        //            {
        //                List<MapNode> list = new List<MapNode>();
        //                list.Add(node);
        //                mappings.Add(key, list);
        //            }
                        
        //        }
        //    }
        //}
    }
}
