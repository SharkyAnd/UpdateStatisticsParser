using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UpdateStatisticsParserLib.Models
{
    public class UpdateFile
    {
        public long Id { get; set; }
        public string FileName { get; set; }
        public string DirectoryName { get; set; }
        public string ServerName { get; set; }
        public int RowsCount { get; set; }
        public FileInfo FileInstance { get; set; }
        public FileType FileType { get; set; }
        public string SessionId { get; set; }
    }

    public enum FileType
    {
        Common,
        Log
    }
}
