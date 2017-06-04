using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using VDS.RDF;
using VDS.RDF.Ontology;
using VDS.RDF.Parsing;
using VDS.RDF.Query;
using VDS.RDF.Query.Algebra;
using VDS.RDF.Query.Inference;
using VDS.RDF.Query.Patterns;

namespace SPARQLtoSQL
{
    public class QuantumQueryProcessor
    {
        Dictionary<string,string> dbURIs = new Dictionary<string, string>();
        public string OntologyFileName { get; private set; }

        public QuantumQueryProcessor(string ontologyFileName)
        {
            if (File.Exists(ontologyFileName))
            {
                this.OntologyFileName = ontologyFileName;
            }
            else throw new FileNotFoundException($"File {ontologyFileName} does not exist!");
        }

        /// <summary>
        /// Stores information about a database and its loader
        /// </summary>
        /// <param name="connString">Database connection string</param>
        /// <param name="dbURI">Unified Resource Identifier for this DB</param>
        /// <param name="dbLoaderType">Database loader (extractor) for this particular database</param>
        public void AddDBInfo(string connString, string dbURI, Type dbLoaderType)
        {
            DBLoaderFactory.RegisterDBLoaders(dbLoaderType, connString);
            dbURIs.Add(dbURI, connString);
        }


        private string GetConnStringFromURI(Dictionary<string, string> dbURIs, string uri)
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

        private bool IsLiteralValue(string value)
        {
            value = value.TrimStart().TrimEnd();
            if (string.IsNullOrEmpty(value) || string.IsNullOrWhiteSpace(value))
                return false;
            bool hasQuotes = (value[0] == '"' && value[value.Length - 1] == '"');
            bool hasAngleBracket = (value[0] == '<' && value[value.Length - 1] == '>');

            return (hasQuotes || !hasAngleBracket) && !Uri.IsWellFormedUriString(value, UriKind.Absolute);
        }

        void ResolveBGPsFromDB(ISparqlAlgebra algebra, IGraph g, Dictionary<string, string> dbURIs, DBLoaderFactory dbLoaderFactory, List<Triple> triplesToAdd)
        {
            if (algebra is IBgp)
            {
                HashSet<string> IFPs = GetIFPsFromOntology(g);

                OntologyGraph ontoGraph = new OntologyGraph();
                ontoGraph.Merge(g);

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

                foreach (TriplePattern triple in triples)
                {
                    //do work here for each triple

                    string subjConnString = GetConnStringFromURI(dbURIs, triple.Subject.ToString());
                    string predConnString = GetConnStringFromURI(dbURIs, triple.Predicate.ToString());
                    string objConnString = GetConnStringFromURI(dbURIs, triple.Object.ToString());
                    #region FEDERATED SCHEMA PROCESSING
                    if (subjConnString == null && predConnString == null && objConnString == null)
                    {
                        //we deal with request to FEDERATED schema or it's an error
                        //if it's FEDERATED schema, we should find subclasses/subproperties, equivalent classes/properties and query for them

                        if (triple.Subject.VariableName == null) //is not a pattern
                        {
                            //IN FEDERATED schema there're no individuals, so we can't have subject URI or subject literal here!

                            //subject here could not be a URI! it would be logically incorrect!!!!!
                            //it could only be a pattern

                            throw new InvalidOperationException("Subject variable in tripple, referring to FEDERATED schema should be a PATTERN!");
                        }
                        if (triple.Predicate.VariableName == null) //is not a pattern
                        {
                            //query for equivalent properties
                            TripleStore store = new TripleStore();
                            store.Add(g);


                            //---------------------------------------------------------------------------------
                            //--------------------------PROCESS owl:equivalentProperty-------------------------
                            var results = GetEquivalentProperties(triple.Predicate.ToString(), store);
                            Console.WriteLine();

                            //object can be a literal or a pattern
                            foreach (SparqlResult resultPredicate in results)
                            {
                                //query with new predicates and transform the results to FEDERATED schema syntax
                                SparqlParameterizedString queryString = new SparqlParameterizedString();
                                SparqlResultSet resultSet = null;
                                if (IsLiteralValue(triple.Object.ToString()))
                                {
                                    queryString.CommandText = $"SELECT * WHERE {{ ?subj <{resultPredicate[0].ToString()}> {triple.Object.ToString()} }} ";
                                    resultSet = QuerySparqlFromDB(g, queryString, dbURIs, triplesToAdd);
                                }
                                else
                                {
                                    if (triple.Object.VariableName == null) //object is not a pattern
                                    {
                                        //if object is a URI (i.e. ?subj <fedPred> <fedObj>) which corresponds to federated schema
                                        //we should resolve underlying equivalent OBJECT subproperties for <fedPred> 
                                        string objectUri_srcSchema = null;
                                        try
                                        {
                                            objectUri_srcSchema = ConvertFederatedIndividualUriToSource(g, triple.Object.ToString(), resultPredicate[0].ToString());
                                        }
                                        catch (InvalidOperationException)
                                        {
                                            continue;
                                        }
                                        queryString.CommandText = $"SELECT * WHERE {{ ?subj <{resultPredicate[0].ToString()}> <{objectUri_srcSchema}> }} ";
                                        resultSet = QuerySparqlFromDB(g, queryString, dbURIs, triplesToAdd);
                                    }
                                    else //object is a pattern
                                    {
                                        queryString.CommandText = $"SELECT * WHERE {{ ?subj <{resultPredicate[0].ToString()}> {triple.Object.VariableName} }} ";
                                        resultSet = QuerySparqlFromDB(g, queryString, dbURIs, triplesToAdd);
                                    }
                                }

                                string federatedStem = triple.Predicate.ToString().Trim('<', '>').Split('#')[0]; //left part (before '#') -> /FEDERATED/table name
                                //federatedStem += '/'; // /FEDERATED/table name/

                                foreach (SparqlResult result in resultSet)
                                {
                                    //convert to federated shema syntax
                                    Dictionary<string, string> dbInfo = GetDatabaseInfoForIndividualURI(result[0].ToString());
                                    string subjStr = $"{federatedStem}/{dbInfo["dbName"]}.{dbInfo["columnName"]}.{dbInfo["columnValue"]}";
                                    string predStr = triple.Predicate.ToString().Trim('<', '>');
                                    string objStr;
                                    if (triple.Object.VariableName == null) //not a pattern
                                    {
                                        if (IsLiteralValue(triple.Object.ToString()))
                                        {
                                            objStr = triple.Object.ToString().Trim('"');
                                        }
                                        else
                                            objStr = triple.Object.ToString().Trim('<', '>');
                                    }
                                    else //object was a pattern ?object in sparql query
                                    {
                                        //dbInfo = GetDatabaseInfoForIndividualURI(result[1].ToString());
                                        if (IsLiteralValue(result[1].ToString()))
                                        {
                                            objStr = result[1].ToString();
                                        }
                                        else
                                        {
                                            dbInfo = GetDatabaseInfoForIndividualURI(result[1].ToString());
                                            string sourceSchemaURI = $"{dbInfo["prefix"]}{dbInfo["dbName"]}/{dbInfo["tableName"]}";
                                            HashSet<string> superClass = GetSuperOntologyClassesURI(g, sourceSchemaURI); //super class=federated class
                                            superClass.RemoveWhere(oclassUri => GetSuperOntologyClassesURI(g, oclassUri).Count > 0); //remove not supreme classes (possibly, not federated schema)
                                            //objStr = $"{federatedStem}/{dbInfo["dbName"]}.{dbInfo["columnName"]}.{dbInfo["columnValue"]}";
                                            if(superClass.Count>0)
                                            {
                                                objStr = $"{superClass.First()}/{dbInfo["dbName"]}.{dbInfo["columnName"]}.{dbInfo["columnValue"]}";
                                            }
                                            else
                                            {
                                                //possibly, logicaly incorrect
                                                objStr = $"{federatedStem}/{dbInfo["dbName"]}.{dbInfo["columnName"]}.{dbInfo["columnValue"]}";
                                            }
                                        }
                                    }


                                    //USE ANOTHER MATCHING TECHNIQUES HERE (i.e. lexicographical matching)

                                    //вместо IFPs.Contains(predStr) можно сделать функцию лексикографического расстояния
                                    //тоесть, если LexDistance(IFPs, predStr) <= someThreshold, считать записи одинаковыми
                                    //только, вместо IFPs должна быть другая таблица с названиями аттрбутов (predicates)
                                    //которые мы хотим сопоставить, а  вместо matchIFPDict должна быть таблица с парами
                                    //[значение аттрибута, который мы хотим сопоставить] <-> значение subject'a
                                    //возможно, стоит придумать свой онтологический аттрибут, чтобы помечать поля, которые
                                    //мы сопоставляем?

                                    //а помечать такие (похожие) поля может программа, которая составляет онтологию с помощью
                                    //общедоступных онтологий (сопосталяемые классы должны обязательно наследоветь FEDERATED класс)

                                    //here we obtain triples from underling DBs - KMS+LMS
                                    //these triples should be matched here ON Inverse Functional Properties
                                    AddRawTripleToList(g, triplesToAdd, subjStr, predStr, objStr);
                                }
                            }
                            //throw new NotImplementedException();
                        }
                        if (triple.Object.VariableName == null) //is not a pattern
                        {
                            if (triple.Subject.VariableName != null) //?subject is a pattern
                            {
                                #region <predicate>
                                if (triple.Predicate.VariableName == null) //is not a pattern
                                {
                                    if (!IsLiteralValue(triple.Object.ToString()))
                                    {
                                        // ?subject <predicate> <object> [FEDERATED SCHEMA]
                                        //<predicate> must refer to an OBJECT PROPERTY!!! otherwise it's a logical error!

                                        //Этот случай уже рассматривался выше - do nothing
                                    }
                                    else
                                    {
                                        //object is a literal value
                                        // ?subject <predicate> "object"

                                        //Этот случай уже рассматривался выше - do nothing
                                    }
                                }
                                #endregion
                                else //predicate is a pattern
                                {
                                    //TODO: resolve predicates and recur, so that <predicate><object> would be triggered
                                    if (!IsLiteralValue(triple.Object.ToString()))
                                    {
                                        //?subject ?predicate <object>
                                        //?subject ?predicate <http://www.example.org/FEDERATED/User/KMS.Id_User.95>
                                        //predicates - are federated properties, which range is http://www.example.org/FEDERATED/User
                                        OntologyGraph ograph = new OntologyGraph();
                                        ograph.Merge(g);
                                        Dictionary<string, string> objectInfo = GetDatabaseInfoForIndividualURI(triple.Object.ToString().Trim('>', '<'));
                                        string tableUri = $"{objectInfo["prefix"]}{objectInfo["dbName"]}/{objectInfo["tableName"]}"; //http://www.example.org/FEDERATED/User
                                        OntologyProperty oprop = new OntologyProperty(new Uri(tableUri), ograph);

                                        //ograph.GetTriplesWithPredicateObject()
                                        OntologyProperty classOfObjProperties = ograph.CreateOntologyProperty(ograph.CreateUriNode("owl:ObjectProperty"));

                                        var federatedPropsWithRange = ograph.GetTriplesWithPredicateObject(
                                                                                    ograph.CreateUriNode("rdfs:range"),
                                                                                    ograph.CreateUriNode(new Uri(tableUri))
                                                                      ).ToList();

                                        foreach (var prop in federatedPropsWithRange)
                                        {
                                            string federatedPredicate = prop.Subject.ToString().Trim('<', '>'); //http://www.example.org/FEDERATED/Admin#Id_User
                                            SparqlParameterizedString queryString = new SparqlParameterizedString();
                                            queryString.CommandText = $"SELECT * WHERE {{ ?subj <{federatedPredicate}> {triple.Object.ToString()} }}"; //SELECT * WHERE { ?subj <http://www.example.org/FEDERATED/Admin#Id_User> <http://www.example.org/FEDERATED/User/KMS.Id_User.95> }
                                            SparqlResultSet resultSet = QuerySparqlFromDB(g, queryString, dbURIs, triplesToAdd); //we supply current list of triples to add, so we do not need the result set
                                        }
                                        //throw new NotImplementedException();
                                    }
                                    else
                                    {
                                        //?subject ?predicate "object"
                                        //this particular query cannot happen for FEDERATED query, because there's no sign that this is a 
                                        //federated query; neither ?subject nor ?predicate references federated query
                                        //THIS METHOD COULD BE REALIZED, BUT IT IT SENSLESS in terms of perfomance,
                                        //because we than should literaly query ALL database records to find that particular "object"
                                        throw new NotImplementedException(@"This particular query cannot happen for FEDERATED query, 
                                                                            because there's no sign that this is a federated query; 
                                                                            neither ?subject nor ?predicate references federated query.");
                                    }
                                }
                            }
                            else    //<subject> is not a pattern
                            {
                                //maybe, it would be logically incorrect to implement this
                                //federated IDs are generated at runtime, so if users would store them for future queries
                                //in next versions of software, these IDs could become obsolete and not supported
                                throw new NotImplementedException();
                            }

                        }
                        //throw new NotImplementedException();
                    }
                    #endregion
                    if (subjConnString != null) //most probably, subjectConnString will be NULL (cause subject may be a pattern)
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

                        IDBLoader dbLoader = dbLoaderFactory.GetDBLoader(subjConnString);
                        Dictionary<string, string> dbInfo = GetDatabaseInfoForIndividualURI(triple.Subject.ToString());

                        List<RawTriple> rawTriples = dbLoader.GetTriplesForSubject(
                            tableName: dbInfo["tableName"],
                            individualColName: dbInfo["columnName"],
                            individualColValue: dbInfo["columnValue"],
                            prefixURI: dbInfo["prefix"] + dbInfo["dbName"],
                            predicateColName: triple.Predicate.VariableName == null ? GetPrefixDbNameTableNameColNameFromURI(triple.Predicate.ToString())["columnName"] : null,
                            obj: triple.Object.VariableName == null ? triple.Object.ToString().Trim('>', '<') : null
                        );
                        foreach (var rawTriple in rawTriples)
                        {
                            AddRawTripleToList(g, triplesToAdd, rawTriple);
                        }
                    }
                    if (predConnString != null)
                    {
                        //referring to DataType- or Object- Property
                        if (triple.Object.VariableName == null) //object is not a pattern
                        {
                            if (IsLiteralValue(triple.Object.ToString()))
                            {
                                // ?s <predicate> "Object"

                                /*
                                    SELECT *
                                    FROM table(predicate)
                                    WHERE table.Attribute="Object"
                                */
                                IDBLoader dbLoader = dbLoaderFactory.GetDBLoader(predConnString);
                                Dictionary<string, string> dbInfo = GetPrefixDbNameTableNameColNameFromURI(triple.Predicate.ToString());

                                List<RawTriple> rawTriples = dbLoader.GetTriplesForPredicateObject(
                                    tableName: dbInfo["tableName"],
                                    columnName: dbInfo["columnName"],
                                    prefixURI: dbInfo["prefix"] + dbInfo["dbName"],
                                    obj: triple.Object.ToString(),
                                    IFPs: GetIFPsFromOntology(g).ToList());

                                foreach (var rawTriple in rawTriples)
                                {
                                    AddRawTripleToList(g, triplesToAdd, rawTriple);
                                }
                            }
                            else if (objConnString != null) //object is a URI
                            {
                                // ?s <predicate> <object>
                                /*
                                    example:
                                    ?s <http://www.semanticweb.org/LMS/User#Role> <http://www.semanticweb.org/LMS/Role/ID.1>
                                    <predicate> here should be an object property

                                    1. <predicate> -> Find in [LMS].User table the FK (attribute name) to [LMS].Role table -> ROLE_ID (FK)
                                    2. 
                                        SELECT *
                                        FROM [LMS].User
                                        WHERE User.ROLE_ID=1
                                */
                                List<RawTriple> rawTriples = null;
                                rawTriples = ResolveTriplesForPredObj_ObjectProperty(dbLoaderFactory, ontoGraph, triple, predConnString);

                                foreach (var rawTriple in rawTriples)
                                {
                                    AddRawTripleToList(g, triplesToAdd, rawTriple);
                                }
                                //throw new NotImplementedException();
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
                            List<RawTriple> rawTriples = null;

                            IDBLoader dbLoader = dbLoaderFactory.GetDBLoader(predConnString);
                            Dictionary<string, string> dbInfo = GetPrefixDbNameTableNameColNameFromURI(triple.Predicate.ToString());
                            //check if predicate is an object property
                            OntologyProperty oprop = TryResolveObjectProperty(ontoGraph, triple.Predicate.ToString());
                            if (oprop != null) //predicate refers to an object property
                            {
                                string propRange = oprop.Ranges.First().Resource.ToString();
                                string rhsTableName = propRange.Substring(propRange.LastIndexOf('/') + 1);

                                if (dbLoader.GetTableNames().Contains(rhsTableName))
                                {
                                    rawTriples = dbLoader.GetTriplesForPredicateObject_ObjProperty(
                                        lhsTableName: dbInfo["tableName"],
                                        nmTableName: dbInfo["columnName"], //it's mapped as a column name
                                        rhsTableName: rhsTableName,   //is inferred from rdfs:range property of object property in ontology
                                        prefixURI: dbInfo["prefix"] + dbInfo["dbName"],
                                        obj: null);
                                }
                                else //rhsTableName refers not to table but to foaf:email, for example
                                {
                                    //try DataTypeProperty method
                                    rawTriples = dbLoader.GetTriplesForPredicateObject(
                                        tableName: dbInfo["tableName"],
                                        columnName: dbInfo["columnName"],
                                        prefixURI: dbInfo["prefix"] + dbInfo["dbName"],
                                        obj: null,
                                        IFPs: GetIFPsFromOntology(g).ToList());
                                }
                                
                            }
                            else {  //a dataType property
                                rawTriples = dbLoader.GetTriplesForPredicateObject(
                                    tableName: dbInfo["tableName"],
                                    columnName: dbInfo["columnName"],
                                    prefixURI: dbInfo["prefix"] + dbInfo["dbName"],
                                    obj: null,
                                    IFPs: GetIFPsFromOntology(g).ToList());
                            }

                            foreach (var rawTriple in rawTriples)
                            {
                                AddRawTripleToList(g, triplesToAdd, rawTriple);
                            }
                        }

                    }
                    if (objConnString != null) //object is not a pattern
                    {
                        //TODO
                        //in ?subject <predicate><object>, <predicate> refers to an OBJECT PROPERTY!

                        List<RawTriple> rawTriples = null;
                        rawTriples = ResolveTriplesForPredObj_ObjectProperty(dbLoaderFactory, ontoGraph, triple, predConnString);

                        foreach (var rawTriple in rawTriples)
                        {
                            AddRawTripleToList(g, triplesToAdd, rawTriple);
                        }
                    }

                    //check if subj, pred and obj refer to one DB
                }
            }
            else
            {
                if (algebra is IUnaryOperator)
                {
                    algebra = algebra as IUnaryOperator;
                    ResolveBGPsFromDB((algebra as IUnaryOperator).InnerAlgebra, g, dbURIs, dbLoaderFactory, triplesToAdd);
                }
                else if (algebra is IAbstractJoin)
                {
                    ResolveBGPsFromDB((algebra as IAbstractJoin).Lhs, g, dbURIs, dbLoaderFactory, triplesToAdd);
                    ResolveBGPsFromDB((algebra as IAbstractJoin).Rhs, g, dbURIs, dbLoaderFactory, triplesToAdd);
                }
            }
        }

        /// <summary>
        /// Converts federated uri of individual to source schema uri, based on source predicateURI
        /// </summary>
        /// <param name="federatedIndividualUri">URI of individual in federated schema terms</param>
        /// <param name="predicateUriSource">Predicate in source schema terms</param>
        /// <returns>Source schema URI of individual</returns>
        private  string ConvertFederatedIndividualUriToSource(IGraph g, string federatedIndividualUri, string predicateUriSource)
        {
            federatedIndividualUri = federatedIndividualUri.Trim('<', '>');
            predicateUriSource = predicateUriSource.Trim('>', '<');
            //http://www.example.org/KMS/Admin#Id_User - predicateUriSource
            //"http://www.example.org/FEDERATED/Admin/KMS.Id_User.95" - federatedIndividualUri
            //syntax of federatedIndividualUri: prefix/XXXXX/TableName/KMS.ID.1.LMS.ID.5

            Dictionary<string, string> dbInfo = GetPrefixDbNameTableNameColNameFromURI(predicateUriSource);
            string schemaReferredByPredicate = dbInfo["dbName"]; //KMS
                                                                 //resolve from federatedIndividualUri PK value for appropriate schema, i.e. in this example ID for KMS = 1 [KMS.ID.1]
                                                                 //if EXCEPTION ArgumentOutOfRange -> federatedUri cannot be converted to source URI with this predicateUriSource
            string srcSchemaPkId = null;
            try
            {
                srcSchemaPkId = federatedIndividualUri.Substring(federatedIndividualUri.IndexOf(schemaReferredByPredicate));
            }
            catch (ArgumentOutOfRangeException)
            {
                throw new InvalidOperationException($"federatedUri={federatedIndividualUri} cannot be converted to source URI with this predicateUriSource={predicateUriSource}");
            }
            Regex r = new Regex(@"(\w+)\.(\w+\.\w+)");    //match DbName, PkName, PkValue, i.e. DbName = KMS, PkName = ID, PkValue = 1 (KMS.ID.1)
            if (!r.IsMatch(srcSchemaPkId))
            {
                throw new ArgumentOutOfRangeException($"federatedIndividualUri={federatedIndividualUri} is invalid individual URI!");
            }
            Match m = r.Match(srcSchemaPkId);
            srcSchemaPkId = m.Groups[2].ToString();

            //now resolve table name of object. It equals to range of predicate, which corresponds to referred schema (i.e. KMS)
            OntologyGraph ograph = new OntologyGraph();
            ograph.Merge(g);

            OntologyProperty oprop = new OntologyProperty(new Uri(predicateUriSource), ograph);
            IEnumerable<OntologyClass> ranges = oprop.Ranges;
            string tableUri = (from range in ranges where range.ToString().Contains($"{dbInfo["prefix"] + dbInfo["dbName"]}") select range.ToString()).FirstOrDefault();
            if (tableUri == null)
            {
                throw new InvalidOperationException($"federatedUri={federatedIndividualUri} cannot be converted to source URI with this predicateUriSource={predicateUriSource}");
            }

            string result = $"{tableUri}/{srcSchemaPkId}";
            return result;
        }

        private  List<string> GetUnderlyingSourceSchemaPropertyURIs(IGraph g, string federatedPropertyURI)
        {
            OntologyGraph ograph = new OntologyGraph();
            ograph.Merge(g);

            OntologyProperty oprop = new OntologyProperty(new Uri(federatedPropertyURI), ograph);
            IEnumerable<OntologyProperty> subProps = oprop.SubProperties;

            List<string> result = new List<string>();
            foreach (OntologyProperty prop in subProps)
            {
                result.Add(prop.ToString().Trim('<', '>'));
            }
            return result;
        }

        private  List<RawTriple> ResolveTriplesForPredObj_ObjectProperty(DBLoaderFactory dbLoaderFactory, OntologyGraph ontoGraph,
                                                                               TriplePattern triple, string predConnString)
        {
            List<RawTriple> rawTriples;
            IDBLoader dbLoader = dbLoaderFactory.GetDBLoader(predConnString);
            Dictionary<string, string> dbInfo = GetPrefixDbNameTableNameColNameFromURI(triple.Predicate.ToString());
            //check if predicate is an object property
            OntologyProperty oprop = TryResolveObjectProperty(ontoGraph, triple.Predicate.ToString());

            if (oprop != null) //predicate refers to an object property
            {
                string propRange = oprop.Ranges.First().Resource.ToString();
                string rhsTableName = propRange.Substring(propRange.LastIndexOf('/') + 1);
                string objString = null;
                if (triple.Object.VariableName == null)   //is not a pattern => should have object URI- string
                {
                    objString = triple.Object.ToString(); //a URI, resolve column name from individual URI string
                    //objString is like <http://www.example.org/KMS/Group/ID.1>, we have to extract 'ID' part
                    string colName = objString.Substring(startIndex: 0, length: objString.LastIndexOf('.'));
                    colName = colName.Substring(startIndex: colName.LastIndexOf('/') + 1);

                    string colValue = objString.Substring(startIndex: objString.LastIndexOf('.') + 1).TrimEnd('>');

                    rawTriples = dbLoader.GetTriplesForPredicateObject_ObjProperty(
                        lhsTableName: dbInfo["tableName"],
                        nmTableName: dbInfo["columnName"], //it's mapped as a column name
                        rhsTableName: rhsTableName,   //is inferred from rdfs:range property of object property in ontology
                        prefixURI: dbInfo["prefix"] + dbInfo["dbName"],
                        colName: colName,
                        obj: colValue);
                }
                else {
                    rawTriples = dbLoader.GetTriplesForPredicateObject_ObjProperty(
                        lhsTableName: dbInfo["tableName"],
                        nmTableName: dbInfo["columnName"], //it's mapped as a column name
                        rhsTableName: rhsTableName,   //is inferred from rdfs:range property of object property in ontology
                        prefixURI: dbInfo["prefix"] + dbInfo["dbName"],
                        obj: null);
                }
            }
            else throw new ArgumentException("If <predicate><object> pattern is specified, <predicate> should refer to Object Property!!!");
            return rawTriples;
        }

        /// <summary>
        /// Tries to resolve an object property from the ontology graph by given predicate URI
        /// </summary>
        /// <param name="ontoGraph">Ontology graph, where predicate is defined</param>
        /// <param name="predicate">Predicate URI in format <http://example.org/KMS/User_Group></param>
        /// <returns>ObjectProperty, if predicate URI refers to object property in ontology graph OR NULL, if ObjectProperty was not found</returns>
        private  OntologyProperty TryResolveObjectProperty(OntologyGraph ontoGraph, string predicate)
        {
            OntologyProperty oprop = ontoGraph.OwlObjectProperties.Where(
                                prop => prop.ToString() == predicate.Replace("<", "").Replace(">", "")).FirstOrDefault();

            return oprop;
        }

        private  void AddRawTripleToList(IGraph g, List<Triple> triplesToAdd, string subjStr, string predStr, string objStr)
        {
            RawTriple rt = new RawTriple
            {
                Subj = subjStr,
                Pred = predStr,
                Obj = objStr
            };
            AddRawTripleToList(g, triplesToAdd, rt);
        }

        private  void AddRawTripleToList(IGraph g, List<Triple> triplesToAdd, RawTriple rawTriple)
        {
            INode subj = g.CreateUriNode(new Uri(rawTriple.Subj));
            INode pred = g.CreateUriNode(new Uri(rawTriple.Pred));
            INode obj = null;
            if (IsLiteralValue(rawTriple.Obj))
                obj = g.CreateLiteralNode($"{rawTriple.Obj}");    //!!!!!!!!!!!!!
            else
                obj = g.CreateUriNode(new Uri(rawTriple.Obj));
            triplesToAdd.Add(new Triple(subj, pred, obj));
        }

        private  SparqlResultSet GetEquivalentProperties(string property, TripleStore store)
        {
            SparqlParameterizedString queryString = new SparqlParameterizedString();

            queryString.Namespaces.AddNamespace("owl", new Uri("http://www.w3.org/2002/07/owl#"));
            queryString.CommandText = @"SELECT ?property1 WHERE {
                                                        ?property owl:equivalentProperty ?property1
                                                        filter regex(str(?property), '^" + property.Trim('<', '>') + "')}";

            SparqlQueryParser parser = new SparqlQueryParser();
            SparqlQuery query = parser.ParseFromString(queryString.ToString());

            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);   //process query
            var results = processor.ProcessQuery(query) as SparqlResultSet;
            return results;
        }

        private HashSet<string> GetIFPsFromOntology(IGraph g)
        {
            TripleStore store = new TripleStore();
            store.Add(g);

            //get list of Inverse Functional Properties from ontology
            //SparqlParameterizedString queryString = new SparqlParameterizedString();

            //queryString.Namespaces.AddNamespace("owl", new Uri("http://www.w3.org/2002/07/owl#"));
            //queryString.Namespaces.AddNamespace("rdf", new Uri("http://www.w3.org/1999/02/22-rdf-syntax-ns#"));
            //queryString.CommandText = @"SELECT ?property WHERE {
            //                                   ?property <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <owl:InverseFunctionalProperty>. }";



            //SparqlQueryParser parser = new SparqlQueryParser();
            //SparqlQuery query = parser.ParseFromString(queryString.ToString());

            //ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);   //process query
            //var results = processor.ProcessQuery(query) as SparqlResultSet;

            OntologyGraph ograph = new OntologyGraph();
            ograph.Merge(g);
            List<Triple> ifpTriples = ograph.GetTriplesWithPredicateObject(ograph.CreateUriNode("rdf:type"),
                ograph.CreateUriNode("owl:InverseFunctionalProperty")).ToList();

            HashSet<string> IFPs = new HashSet<string>();
            foreach (Triple result in ifpTriples)
            {
                IFPs.Add(result.Subject.ToString());
            }
            return IFPs;
        }

        private SparqlResultSet QuerySparqlFromDB(IGraph g1, SparqlParameterizedString sparqlQuery, Dictionary<string, string> dbURIs, List<Triple> triplesToAdd)
        {
            IGraph g = new VDS.RDF.Graph(g1.Triples);
            TripleStore store = new TripleStore();
            store.Add(g);

            SparqlQueryParser parser = new SparqlQueryParser();
            SparqlQuery query = parser.ParseFromString(sparqlQuery.ToString());

            //List<Triple> triplesToAdd = new List<Triple>();
            //Dictionary<string, List<string>> matchIFPsDict = new Dictionary<string, List<string>>();

            ResolveBGPsFromDB(query.ToAlgebra(), g, dbURIs, new DBLoaderFactory(), triplesToAdd);
            MatchRecordsIFPAndStore(g, triplesToAdd);

            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);   //process query
            var results = processor.ProcessQuery(query) as SparqlResultSet;
            return results;
        }

        private Dictionary<string, string> GetPrefixDbNameTableNameColNameFromURI(string uri)
        {
            uri = uri.Trim('<', '>');
            Dictionary<string, string> result = new Dictionary<string, string>();
            Regex r = new Regex(@"(http\w{0,1}://.+/)(\w+)/(\w+)#(\w+)");
            if (r.IsMatch(uri))
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

        private Dictionary<string, string> GetDatabaseInfoForIndividualURI(string individualURI)
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

        public void AddStdPrefixes(SparqlParameterizedString pString)
        {
            pString.Namespaces.AddNamespace("owl", new Uri("http://www.w3.org/2002/07/owl#"));
            pString.Namespaces.AddNamespace("rdf", new Uri("http://www.w3.org/1999/02/22-rdf-syntax-ns#"));
            pString.Namespaces.AddNamespace("rdfs", new Uri("http://www.w3.org/2000/01/rdf-schema#"));
            pString.Namespaces.AddNamespace("foaf", new Uri("http://xmlns.com/foaf/0.1/"));
            pString.Namespaces.AddNamespace("dc", new Uri("http://purl.org/dc/elements/1.1/"));
        }

        public Dictionary<string, List<string>> ConvertSparqlResultSetToDict(SparqlResultSet rSet)
        {
            Dictionary<string, List<string>> dict = new Dictionary<string, List<string>>();
            foreach (var variable in rSet.Variables)
            {
                dict.Add(variable.ToString(), new List<string>());
            }
            foreach (SparqlResult result in rSet)
            {
                foreach (var variable in dict.Keys)
                {
                    INode val;// = result.Value(variable);
                    result.TryGetValue(variable, out val);
                    if (val != null)
                    {
                        dict[variable].Add(val.ToString());
                    }
                    else
                    {
                        dict[variable].Add(string.Empty);
                    }
                }
            }
            return dict;
        }

        public DataTable ConvertSparqlResultSetToDataTable(SparqlResultSet rSet)
        {
            DataTable dt = new DataTable();
            foreach (var variable in rSet.Variables) //add headers
            {
                DataColumn dc = new DataColumn(variable.ToString());
                dt.Columns.Add(dc);
            }
            foreach (SparqlResult result in rSet)
            {
                DataRow dr = dt.NewRow();
                foreach (var variable in rSet.Variables)
                {
                    INode val;// = result.Value(variable);
                    result.TryGetValue(variable, out val);
                    if (val != null)
                    {
                        dr[variable.ToString()] = val.ToString();
                    }
                    else
                    {
                        dr[variable.ToString()] = string.Empty;
                    }
                }
                dt.Rows.Add(dr);
            }
            return dt;
        }

        public SparqlResultSet ExecuteSparql(string queryString)
        {
            SparqlParameterizedString queryParameterizedString = new SparqlParameterizedString();
            queryParameterizedString.CommandText = queryString;
            if(!queryString.ToLower().Contains("prefix"))
            {
                AddStdPrefixes(queryParameterizedString);
            }
            return ExecuteSparql(queryParameterizedString);
        }

        public SparqlResultSet ExecuteSparql(SparqlParameterizedString queryParameterizedString)
        {
            return ExecuteSparql(queryParameterizedString, new GenericRecordsAmbiguityResolver()); //use Levenshtein distance to match strings
        }

        public SparqlResultSet ExecuteSparql(SparqlParameterizedString queryParameterizedString, IRecordsAmbiguityResolver ambiguityResolver)
        {
            IGraph g = new VDS.RDF.Graph();                 //Load triples from file OWL
            g.LoadFromFile(this.OntologyFileName);
            g.BaseUri = null; //!

            RdfsReasoner reasoner = new RdfsReasoner(); //Apply reasoner
            reasoner.Initialise(g);
            reasoner.Apply(g);

            g.BaseUri = null;   //!!!!!!!!
            TripleStore store = new TripleStore();
            store.Add(g);

            SparqlQueryParser parser = new SparqlQueryParser();
            SparqlQuery query = parser.ParseFromString(queryParameterizedString.ToString());

            List<Triple> triplesToAdd = new List<Triple>();
            ResolveBGPsFromDB(query.ToAlgebra(), g, dbURIs, new DBLoaderFactory(), triplesToAdd);

            ResolveAmbiguitiesInTriples(triplesToAdd, ambiguityResolver);
            MatchRecordsIFPAndStore(g, triplesToAdd);

            //Console.WriteLine(query.ToAlgebra());
            File.WriteAllText("results_log.txt", "\n" + query.ToAlgebra().ToString());
            Console.WriteLine(query.ToString());

            ISparqlQueryProcessor processor = new LeviathanQueryProcessor(store);   //process query
            var results = processor.ProcessQuery(query) as SparqlResultSet;

            //write to LOG
            if (results is SparqlResultSet)
            {
                SparqlResultSet rset = (SparqlResultSet)results;
                foreach (SparqlResult result in rset)
                {
                    File.AppendAllText("results_log.txt", "\n" + result.ToString());
                }
            }

            return results;
        }

        /// <summary>
        /// Triples are ambiguos if they contain for the same subject and predicate different objects
        /// </summary>
        /// <param name="triplesToAdd">Triples which will be added to target triple store</param>
        /// <param name="resolver">The same records resolver</param>
        private  void ResolveAmbiguitiesInTriples(List<Triple> triplesToAdd, IRecordsAmbiguityResolver resolver)
        {
            Dictionary<string, List<string>> subjPred_ObjectListDict = new Dictionary<string, List<string>>();

            foreach (Triple t in triplesToAdd)
            {
                string key = $"{t.Subject}|{t.Predicate}";
                if (!subjPred_ObjectListDict.ContainsKey(key))
                {
                    subjPred_ObjectListDict.Add(key, new List<string>(new string[] { t.Object.ToString() }));
                }
                else
                {
                    subjPred_ObjectListDict[key].Add(t.Object.ToString());
                }
            }

            foreach (var dKey in subjPred_ObjectListDict.Keys.ToArray())
            {
                if (subjPred_ObjectListDict[dKey].Count < 2)
                    subjPred_ObjectListDict.Remove(dKey);
                else
                {
                    //we've found ambiguity -> resolve it
                    string resolution = subjPred_ObjectListDict[dKey][0];
                    for (int i = 1; i < subjPred_ObjectListDict[dKey].Count; i++)
                    {
                        resolution = resolver.Resolve(resolution, subjPred_ObjectListDict[dKey][i]);
                    }

                    //now put the resolution in the initial triple list
                    Triple problemTriple = triplesToAdd.Find(t => $"{t.Subject}|{t.Predicate}" == dKey);
                    IGraph g = problemTriple.Graph;
                    INode obj = null;// g.CreateLiteralNode(resolution);
                    if (IsLiteralValue(resolution))
                    {
                        obj = g.CreateLiteralNode(resolution);
                    }
                    else
                        obj = g.CreateUriNode(new Uri(resolution));

                    Triple correctTriple = new Triple(problemTriple.Subject, problemTriple.Predicate, obj);
                    triplesToAdd.RemoveAll(t => $"{t.Subject}|{t.Predicate}" == dKey);  //remove ambiguous triples
                    triplesToAdd.Add(correctTriple);    //add correct one instead
                }
            }
        }

        private  void MatchRecordsIFPAndStore(IGraph g, List<Triple> triplesToAddIn)
        {
            HashSet<string> IFPs = GetIFPsFromOntology(g);
            var matchIFPDict = new Dictionary<string, List<string>>();
            List<Triple> triplesToAdd = triplesToAddIn.ToList();
            foreach (var triple in triplesToAdd)
            {
                string predStr = triple.Predicate.ToString();
                string objStr = triple.Object.ToString();
                string subjStr = triple.Subject.ToString();
                if (IFPs.Contains(predStr))
                {
                    if (matchIFPDict.ContainsKey(objStr))
                    {
                        if (!matchIFPDict[objStr].Contains(subjStr))
                            matchIFPDict[objStr].Add(subjStr);
                    }
                    else
                        matchIFPDict.Add(objStr, new List<string>(new string[] { subjStr }));
                }
            }

            if (matchIFPDict.Count > 0)
            {
                //compose unified IDs
                Dictionary<string, string> unifiedIDs = new Dictionary<string, string>();

                foreach (var key in matchIFPDict.Keys)
                {
                    if (matchIFPDict[key].Count < 2)
                        continue;

                    Regex r = new Regex(@"((.+)/(.+)/(.+))/(.+)$");

                    string classUri = r.Match(matchIFPDict[key][0]).Groups[1].Value;
                    HashSet<string> superClassHSet = GetSuperOntologyClassesURI(g, classUri);

                    for (int i = 1; i < matchIFPDict[key].Count; i++)
                    {
                        classUri = r.Match(matchIFPDict[key][i]).Groups[1].Value;

                        HashSet<string> nextSuper = GetSuperOntologyClassesURI(g, classUri);
                        if (nextSuper.Count > 0)
                        {
                            superClassHSet.IntersectWith(nextSuper);
                        }
                        else
                        {
                            matchIFPDict[key].RemoveAt(i); //it should be a FEDERATED class -> remove it from here
                        }
                    }
                    //to final superClassHSet contains only those classes which are superclasses of all classes in matchIFPDict[key] list
                    //we take the first one by default

                    string federated_stem = superClassHSet.FirstOrDefault();
                    if (federated_stem == null || federated_stem.Length == 0)
                        throw new Exception("Federated stem derivation error!");

                    string federated_uri = federated_stem + "/";    ///////!!!!
                    foreach (var subj in matchIFPDict[key])
                    {
                        var match = r.Match(subj);
                        federated_uri += match.Groups[3] + "." + match.Groups[5].Value + ".";
                    }
                    federated_uri = federated_uri.Remove(federated_uri.Length - 1, 1);


                    foreach (var subj in matchIFPDict[key])
                    {
                        if (!unifiedIDs.ContainsKey(subj))
                        {
                            unifiedIDs.Add(subj, federated_uri);
                        }
                    }
                }

                List<string> federatedSubjectsAdded = new List<string>();
                OntologyGraph ograph = new OntologyGraph();
                ograph.Merge(g);
                foreach (Triple t in triplesToAdd.ToList())
                {
                    string subjStr = t.Subject.ToString(); //probably, unique ID
                    if (!unifiedIDs.ContainsKey(subjStr))
                        continue;

                    subjStr = unifiedIDs[subjStr];
                    string objStr = unifiedIDs.ContainsKey(t.Object.ToString()) ? unifiedIDs[t.Object.ToString()] : t.Object.ToString();


                    List<Triple> federatedPredicateTriples = ograph.GetTriplesWithPredicateObject(ograph.CreateUriNode("owl:equivalentProperty"),
                        ograph.CreateUriNode(new Uri(t.Predicate.ToString()))).ToList();

                    INode subj = g.CreateUriNode(new Uri(subjStr));
                    INode obj; // = g.CreateLiteralNode($"{rawTriple.Obj}");
                    if (IsLiteralValue(objStr))
                    {
                        obj = g.CreateLiteralNode(objStr);
                    }
                    else obj = g.CreateUriNode(new Uri(objStr));
                    //g.Assert(subj, t.Predicate, obj);
                    g.Assert(subj, t.Predicate, obj);
                    federatedSubjectsAdded.Add(subjStr);

                    triplesToAdd.Remove(t);
                }

                //add the remaining ones
                foreach (Triple t in triplesToAdd)
                {
                    if (!federatedSubjectsAdded.Contains(t.Subject.ToString(), new StringContainmentComparer()))
                    {
                        g.Assert(t);
                    }
                }
            }
            else
            {
                //IFP is empty, so just store all records
                foreach (Triple t in triplesToAdd)
                {
                    g.Assert(t);
                }
            }
        }

        private class StringContainmentComparer : IEqualityComparer<string>
        {
            public bool Equals(string x, string y)
            {
                return x.Contains(y);
            }

            public int GetHashCode(string obj)
            {
                return obj.GetHashCode();
            }
        }

        private HashSet<string> GetSuperOntologyClassesURI(IGraph ontoGraph, string subClassURI)
        {
            string queryStr = $"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> SELECT ?super WHERE {{ <{subClassURI}> rdfs:subClassOf ?super}}";

            IGraph g = new VDS.RDF.Graph(ontoGraph.Triples);
            RdfsReasoner reasoner = new RdfsReasoner();
            //reasoner.Initialise(g);
            //reasoner.Apply(g);

            //SparqlParameterizedString queryString = new SparqlParameterizedString(query);

            TripleStore store = new TripleStore();
            store.Add(g);

            SparqlQueryParser parser = new SparqlQueryParser();
            SparqlQuery query = parser.ParseFromString(queryStr);
            LeviathanQueryProcessor queryProcessor = new LeviathanQueryProcessor(store);
            SparqlResultSet rset = queryProcessor.ProcessQuery(query) as SparqlResultSet;
            HashSet<string> result = new HashSet<string>();

            foreach (var r in rset)
            {
                result.Add(r.Value(r.Variables.First()).ToString());
            }

            return result;
        }
    }
}
