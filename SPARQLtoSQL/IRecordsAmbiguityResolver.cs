using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPARQLtoSQL
{
    public interface IRecordsAmbiguityResolver
    {
        /// <summary>
        /// Resolves ambiguity between two strings with one semantic meaning
        /// </summary>
        /// <returns>Returns string which is a compromise between s1 and s2</returns>
        string Resolve(string s1, string s2);
    }
}
