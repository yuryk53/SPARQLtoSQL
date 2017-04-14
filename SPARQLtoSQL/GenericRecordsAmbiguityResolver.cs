using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPARQLtoSQL
{
    class GenericRecordsAmbiguityResolver : IRecordsAmbiguityResolver
    {
        //strings which are at edit distance of 10 are treated as equal
        const int similarityThresholdLevenshtein = 10;

        public string Resolve(string s1, string s2)
        {
            if(s1.Contains(s2))
            {
                return s1;
            }
            else if(s2.Contains(s1))
            {
                return s2;
            }
            //else compute Levenshtein distance
            if(LevenshteinDistance.Compute(s1, s2)<=similarityThresholdLevenshtein)
            {
                //then, strings are equal -> return string of greater size
                if (s1.Length > s2.Length)
                    return s1;
                else return s2;
            }
            else
            {
                //strings are of different semantic content -> return concatenated string
                return $"{s1}; {s2}";
            }

        }
    }
}
