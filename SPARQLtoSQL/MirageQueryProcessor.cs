using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using VDS.RDF.Parsing;
using VDS.RDF.Parsing.Handlers;
using VDS.RDF.Query.Algebra;
using VDS.RDF.Query.Construct;
using VDS.RDF.Query.Describe;
using VDS.RDF.Query.Datasets;
using VDS.RDF.Query.Patterns;
using SPARQLtoSQL;

namespace VDS.RDF.Query
{
    /// <summary>
    /// Default SPARQL Query Processor provided by the library's SPARQL Engine
    /// </summary>
    /// <remarks>
    /// <para>
    /// The Mirage Query Processor simply invokes the <see cref="ISparqlAlgebra">Evaluate</see> method of the SPARQL Algebra it is asked to process
    /// </para>
    /// <para>
    /// In future releases much of the Mirage Query engine logic will be moved into this class to make it possible for implementors to override specific bits of the algebra processing but this is not possible at this time
    /// </para>
    /// </remarks>
    public class MirageQueryProcessor
        : ISparqlQueryProcessor
    {

        Mapping mapping = null;
        /// <summary>
        /// Creates a new Mirage Query Processor
        /// </summary>
        /// <param name="store">Triple Store</param>
        public MirageQueryProcessor(Mapping mapping)
        {
            this.mapping = mapping;
        }


        /// <summary>
        /// Processes a SPARQL Query
        /// </summary>
        /// <param name="query">SPARQL Query</param>
        /// <returns></returns>
        public Object ProcessQuery(SparqlQuery query)
        {
            switch (query.QueryType)
            {
                case SparqlQueryType.Ask:
                case SparqlQueryType.Select:
                case SparqlQueryType.SelectAll:
                case SparqlQueryType.SelectAllDistinct:
                case SparqlQueryType.SelectAllReduced:
                case SparqlQueryType.SelectDistinct:
                case SparqlQueryType.SelectReduced:
                    string sql = TranslateSparqlToSQL(query);
                    return sql;
                    
                //case SparqlQueryType.Construct:
                //case SparqlQueryType.Describe:
                //case SparqlQueryType.DescribeAll:
                //    IGraph g = new Graph();
                //    this.ProcessQuery(new GraphHandler(g), null, query);
                //    return g;
                default:
                    throw new RdfQueryException("Cannot process unknown query types");
            }
        }


        private string TranslateSparqlToSQL(SparqlQuery query)
        {
            //Do Handler null checks before evaluating the query
            if (query == null) throw new ArgumentNullException("query", "Cannot evaluate a null query");
            
            //Reset Query Timers
            query.QueryExecutionTime = null;

            //Convert to Algebra and execute the Query
            //SparqlEvaluationContext context = this.GetContext(query);

            ISparqlAlgebra algebra = query.ToAlgebra();
            Console.WriteLine(algebra.ToString());
            //result = context.Evaluate(algebra); //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            return ProcessAlgebra(algebra);
        }

        /// <summary>
        /// Processes a SPARQL Query sending the results to a RDF/SPARQL Results handler as appropriate
        /// </summary>
        /// <param name="rdfHandler">RDF Handler</param>
        /// <param name="resultsHandler">Results Handler</param>
        /// <param name="query">SPARQL Query</param>
        public void ProcessQuery(IRdfHandler rdfHandler, ISparqlResultsHandler resultsHandler, SparqlQuery query)
        {
            throw new NotImplementedException();
        }



        /// <summary>
        /// Delegate used for asychronous execution
        /// </summary>
        /// <param name="rdfHandler">RDF Handler</param>
        /// <param name="resultsHandler">Results Handler</param>
        /// <param name="query">SPARQL Query</param>
        private delegate void ProcessQueryAsync(IRdfHandler rdfHandler, ISparqlResultsHandler resultsHandler, SparqlQuery query);

        /// <summary>
        /// Processes a SPARQL Query asynchronously invoking the relevant callback when the query completes
        /// </summary>
        /// <param name="query">SPARQL QUery</param>
        /// <param name="rdfCallback">Callback for queries that return a Graph</param>
        /// <param name="resultsCallback">Callback for queries that return a Result Set</param>
        /// <param name="state">State to pass to the callback</param>
        /// <remarks>
        /// In the event of a success the appropriate callback will be invoked, if there is an error both callbacks will be invoked and passed an instance of <see cref="AsyncError"/> which contains details of the error and the original state information passed in.
        /// </remarks>
        public void ProcessQuery(SparqlQuery query, GraphCallback rdfCallback, SparqlResultsCallback resultsCallback, Object state)
        {
            Graph g = new Graph();
            SparqlResultSet rset = new SparqlResultSet();
            ProcessQueryAsync d = new ProcessQueryAsync(this.ProcessQuery);
            d.BeginInvoke(new GraphHandler(g), new ResultSetHandler(rset), query, r =>
            {
                try
                {
                    d.EndInvoke(r);
                    if (rset.ResultsType != SparqlResultsType.Unknown)
                    {
                        resultsCallback(rset, state);
                    }
                    else
                    {
                        rdfCallback(g, state);
                    }
                }
                catch (RdfQueryException queryEx)
                {
                    if (rdfCallback != null) rdfCallback(null, new AsyncError(queryEx, state));
                    if (resultsCallback != null) resultsCallback(null, new AsyncError(queryEx, state));
                }
                catch (Exception ex)
                {
                    RdfQueryException queryEx = new RdfQueryException("Unexpected error while making an asynchronous query, see inner exception for details", ex);
                    if (rdfCallback != null) rdfCallback(null, new AsyncError(queryEx, state));
                    if (resultsCallback != null) resultsCallback(null, new AsyncError(queryEx, state));
                }
            }, state);
        }

        /// <summary>
        /// Processes a SPARQL Query asynchronously passing the results to the relevant handler and invoking the callback when the query completes
        /// </summary>
        /// <param name="rdfHandler">RDF Handler</param>
        /// <param name="resultsHandler">Results Handler</param>
        /// <param name="query">SPARQL Query</param>
        /// <param name="callback">Callback</param>
        /// <param name="state">State to pass to the callback</param>
        /// <remarks>
        /// In the event of a success the callback will be invoked, if there is an error the callback will be invoked and passed an instance of <see cref="AsyncError"/> which contains details of the error and the original state information passed in.
        /// </remarks>
        public void ProcessQuery(IRdfHandler rdfHandler, ISparqlResultsHandler resultsHandler, SparqlQuery query, QueryCallback callback, Object state)
        {
            ProcessQueryAsync d = new ProcessQueryAsync(this.ProcessQuery);
            d.BeginInvoke(rdfHandler, resultsHandler, query, r =>
            {
                try
                {
                    d.EndInvoke(r);
                    callback(rdfHandler, resultsHandler, state);
                }
                catch (RdfQueryException queryEx)
                {
                    callback(rdfHandler, resultsHandler, new AsyncError(queryEx, state));
                }
                catch (Exception ex)
                {
                    callback(rdfHandler, resultsHandler, new AsyncError(new RdfQueryException("Unexpected error making an asynchronous query", ex), state));
                }
            }, state);
        }

        #region Algebra Processor Implementation

        /// <summary>
        /// Processes SPARQL Algebra
        /// </summary>
        /// <param name="algebra">Algebra</param>
        /// <param name="context">SPARQL Evaluation Context</param>
        public string ProcessAlgebra(ISparqlAlgebra algebra)
        {
            string sql = "";
            if (algebra is IBgp) //a tripple pattern
            {
                //here use the value from mapping
                Bgp bgp = algebra as Bgp;

                //foreach(TriplePattern triplePattern in bgp.TriplePatterns)
                //{
                //    sql += this.mapping.GetMappingForTripple(triplePattern.Subject.ToString(), triplePattern.Predicate.ToString(), triplePattern.Object.ToString());
                //}
                List<ITriplePattern> patterns = bgp.TriplePatterns.ToList();
                if(patterns.Count>=2)
                {
                    TriplePattern p1 = patterns[0] as TriplePattern;
                    TriplePattern p2 = patterns[1] as TriplePattern;
                    MapNode mNode1 = mapping.GetMapNodeForTripple(p1.Subject.ToString(), p1.Predicate.ToString(), p1.Object.ToString());
                    MapNode mNode2 = mapping.GetMapNodeForTripple(p2.Subject.ToString(), p2.Predicate.ToString(), p2.Object.ToString());
                    //perform inner joins between them
                    string sql1 = mapping.MergeSqlDBString(mNode1);
                    string sql2 = mapping.MergeSqlDBString(mNode2);

                    sql = $"{InnerJoin(sql1, sql2)}";
                    Console.WriteLine("\n\n{0}", sql);
                    //sql += $"{InnerJoin(patterns[0], patterns[1])}";
                }
                else
                {
                    sql += $"({patterns[0]})";
                }
            }
            else if (algebra is Select)
            {
                Select select = algebra as Select;
                string variables = select.IsSelectAll ? "*" : string.Join(",", select.FixedVariables);
                string from = ProcessAlgebra(select.InnerAlgebra);
                sql = $"SELECT {variables}\nFROM ({from}) \nWHERE *";
            }
            //else if (algebra is IFilter)
            //{
            //    return this.ProcessFilter((IFilter)algebra);
            //}
            ////else if (algebra is Algebra.Graph)
            ////{
            ////    return this.ProcessGraph((Algebra.Graph)algebra, context);
            ////}
            //else if (algebra is IJoin)
            //{
            //    return this.ProcessJoin((IJoin)algebra);
            //}
            //else if (algebra is ILeftJoin)
            //{
            //    return this.ProcessLeftJoin((ILeftJoin)algebra);
            //}

            //else if (algebra is IUnion)
            //{
            //    return this.ProcessUnion((IUnion)algebra);
            //}
            //else
            //{
            //    //Unknown Algebra
            //    throw new Exception("ProcessAlgebra(): Unknown algebra!");
            //}
            return sql;
        }

        public string ProcessBGP(Bgp bgp)
        {
            List<ITriplePattern> patterns = bgp.TriplePatterns.ToList();
            if (patterns.Count == 2)
            {
                TriplePattern p1 = patterns[0] as TriplePattern;
                TriplePattern p2 = patterns[1] as TriplePattern;
                MapNode mNode1 = mapping.GetMapNodeForTripple(p1.Subject.ToString(), p1.Predicate.ToString(), p1.Object.ToString());
                MapNode mNode2 = mapping.GetMapNodeForTripple(p2.Subject.ToString(), p2.Predicate.ToString(), p2.Object.ToString());
                //perform inner joins between them
                string sql1 = mapping.MergeSqlDBString(mNode1);
                string sql2 = mapping.MergeSqlDBString(mNode2);

                string sql = $"{InnerJoin(sql1, sql2)}";
                return sql;
                //sql += $"{InnerJoin(patterns[0], patterns[1])}";
            }
            else if(patterns.Count == 1)
            {
                TriplePattern p = patterns[0] as TriplePattern;
                MapNode mNode = mapping.GetMapNodeForTripple(p.Subject.ToString(), p.Predicate.ToString(), p.Object.ToString());
                string sqlTripple = mapping.MergeSqlDBString(mNode);
                string sql = $"({sqlTripple})";
                return sql;
            }
            else if(patterns.Count > 2)
            {
                //here we run the cyclic joins on each 2 queries,
                //optimizing the queries after each join
            }
            return null;
        }

        public string InnerJoin(string q1, string q2)
        {
            return $"SELECT Q1.x FROM\n ({q1}) Q1\nJOIN\n({q2}) Q2\nON Q1.x=Q2.x";
        }

        #endregion
    }
}
