using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UpdateStatisticsParserLib.Helpers
{
    public static class ExtensionMethods
    {
        public static string NormalizeDistributive(this string distrNumber)
        {
            while (distrNumber.StartsWith("0"))
                distrNumber = distrNumber.Remove(0, 1);
            return distrNumber;
        }
    }
}
