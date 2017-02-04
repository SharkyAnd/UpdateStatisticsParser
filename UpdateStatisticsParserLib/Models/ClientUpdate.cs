using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UpdateStatisticsParserLib.Models
{
    public class ClientUpdate
    {
        public int? Id { get; set; }
        public int? DistributiveId { get; set; }
        public string DistributiveNumber { get; set; }
        public string SessionId { get; set; }
    }
}
