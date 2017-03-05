using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SPARQLtoSQL
{
    public class MapNode
    {
        public string DBString { get; set; } //Subject //format: <example.com/db1/neoplasm/{pid}> where {pid} - PK
        public string Predicate { get; set; }
        public string Object { get; set; }
        public string SQL { get; set; }

        public bool IsMatch(string subj, string pred, string obj)
        {
            if (subj.Length == 0 || pred.Length == 0 || obj.Length == 0)
                return false;

            Regex r = new Regex(@"\{.+\}");
            //check subject
            bool matchSubj = r.IsMatch(DBString) && subj[0] == '?';
            bool matchPred = pred.CompareTo(Predicate) == 0;
            bool matchObj = r.IsMatch(Object) || obj.CompareTo(Object) == 0;

            return matchSubj && matchPred && matchObj;
        }
    }

    public class Mapping
    {
        public Dictionary<string, List<MapNode>> mappings = new Dictionary<string, List<MapNode>>();
        public Dictionary<string, string> prefixes = new Dictionary<string, string>();

        public Mapping(Dictionary<string, List<MapNode>> map)
        {
            this.mappings = map;
            AddStdPrefixes();
        }

        public Mapping() { AddStdPrefixes(); }

        private void AddStdPrefixes()
        {
            prefixes["rdf"] = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#>";
        }

        public string GetMappingForTripple(string subj, string pred, string obj)
        {
            return GetMapNodeForTripple(subj, pred, obj).SQL;
        }

        public MapNode GetMapNodeForTripple(string subj, string pred, string obj)
        {
            //try to match each node to sql expressions from mapping
            string key = $"{pred} {obj}";
            if (mappings.ContainsKey(key))
            {
                return mappings[key][0]; 
            }
            else
            {
                key = $"{pred}";
                if (mappings.ContainsKey(key))
                {
                    return mappings[key].Find(mapNode => mapNode.Object == obj);
                    
                }
                else //there is no mapping for this node
                {
                    throw new Exception($"No mapping for node {subj} {pred} {obj} was provided!");
                }
            }
        }

        private string NormalizePrefix(string spo)
        {
            string[] predicateParts = spo.Split(':');
            string predicate = "";
            if (predicateParts.Length == 2) //append prefix
            {
                if (prefixes.ContainsKey(predicateParts[0]))
                {
                    predicate = $"<{prefixes[predicateParts[0]].Trim('<', '>')}{predicateParts[1]}>";
                }
                else throw new InvalidDataException($"Unknown prefix '{predicateParts[0]}'! Maybe, it's not defined?");
            }
            return predicate;
        }

        public string UnPrefix(string var)
        {
            StringBuilder sb = new StringBuilder(var);

            while(sb.Length > 2) //while var!= "<>"
            {
                sb = sb.Remove(sb.Length - 2, 1);
                if(prefixes.ContainsValue(sb.ToString()))
                {
                    return var.Substring(sb.Length-1, var.Length - sb.Length);
                }
            }
            throw new ArgumentOutOfRangeException($"Can't UnPrefix {var}. There's no such prefix");
        }

        public void ReadMappings(string fName)
        {
            using (StreamReader sr = new StreamReader(new FileStream(fName, FileMode.Open)))
            {
                while (!sr.EndOfStream)
                {
                    //process node by node
                    string[] nodeLine = sr.ReadLine().Split();

                    if(nodeLine[0] == "PREFIX")
                    {
                        prefixes[nodeLine[1].Replace(":", "")] = nodeLine[2];
                        continue;
                    }

                    //read SQL lines
                    string sql = "";
                    while (sr.Peek() == '\t')
                    {
                        sql += " " + sr.ReadLine().Remove(0, 1);
                    }
                    sql = sql.Trim(new char[] { ' ', '.' });


                    nodeLine[0] = NormalizePrefix(nodeLine[0]);
                    nodeLine[1] = NormalizePrefix(nodeLine[1]);
                    nodeLine[2] = NormalizePrefix(nodeLine[2]);

                    MapNode node = new MapNode
                    {
                        DBString = nodeLine[0],
                        Predicate = nodeLine[1],
                        Object = nodeLine[2],
                        SQL = sql
                    };

                    Regex r = new Regex(@"\{.+\}");


                    string key = "";
                    if (r.IsMatch(nodeLine[1]))
                    {
                        key = nodeLine[2];
                    }
                    else if (r.IsMatch(nodeLine[2]))
                    {
                        key = nodeLine[1];
                    }
                    else
                        key = $"{nodeLine[1]} {nodeLine[2]}";

                    if (mappings.ContainsKey(key))
                    {
                        mappings[key].Add(node);
                    }
                    else
                    {
                        List<MapNode> list = new List<MapNode>();
                        list.Add(node);
                        mappings.Add(key, list);
                    }

                }
            }
        }

        public string MergeSqlDBString(MapNode node)
        {
            string unPrefDBString = UnPrefix(node.DBString);
            Regex pkRegex = new Regex(@"\{.+\}");
            string PK = pkRegex.Match(unPrefDBString).ToString();
            unPrefDBString = unPrefDBString.Replace(PK, "");
            PK = PK.Trim('{', '}');

            string sql = (string)node.SQL.Clone();
            return sql.Replace(PK, $"concat(\"{unPrefDBString}\", {PK}) AS x");
        }
    }
}
