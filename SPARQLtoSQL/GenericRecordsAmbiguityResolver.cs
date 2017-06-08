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
