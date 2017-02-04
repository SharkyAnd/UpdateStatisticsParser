using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UpdateStatisticsParserLib.Models;
using UpdateStatisticsParserLib.Workers;

namespace UpdateStatisticsParserLib
{
    /// <summary>
    /// Главный класс приложения
    /// </summary>
    public class UpdateStatisticsParserWorker
    {
        private UpdateStatisticWorkerMode _updateStatWorkerMode = UpdateStatisticWorkerMode.Online;
        public UpdateStatisticWorkerMode UpdateStatWorkerMode
        {
            get { return _updateStatWorkerMode; }
            set { _updateStatWorkerMode = value; }
        }

        private string _specificFolderPath = UpdateStatisticsParserConfig.Instance.LogFilesFolders.FirstOrDefault().Value;
        public string SpecificFolderPath
        {
            get { return _specificFolderPath; }
            set { _specificFolderPath = value; }
        }

        private string _specificFilePath;
        public string SpecificFilePath
        {
            get { return _specificFilePath; }
            set { _specificFilePath = value; }
        }

        private int _numberOfDays;
        public int NumberOfDays
        {
            get { return _numberOfDays; }
            set { _numberOfDays = value; }
        }

        public enum UpdateStatisticWorkerMode
        {
            UpdateInfoFromCommonLogFile,
            GetUsrFiles,
            ParseAllFolder,
            ParseNFolders,
            ParseSpecificFolder,
            Online,
            Recovery,
            Repair
        }

        private List<FileParserBase> _fileParsers = new List<FileParserBase> { new LogFilesParser(), new CommonLogFilesParser() };
        public List<FileParserBase> FileParsers
        {
            get { return _fileParsers; }
            set { _fileParsers = value; }
        }

        public void Start()
        {
            LogManager.GetCurrentClassLogger().Info(@"Режим работы приложения: {0}", _updateStatWorkerMode.ToString());

            switch (_updateStatWorkerMode)
            {
                case UpdateStatisticWorkerMode.Online:
                    foreach (var parser in _fileParsers)
                        parser.StartWatching();
                    break;
                case UpdateStatisticWorkerMode.ParseAllFolder:
                    foreach (var parser in _fileParsers)
                        if (parser is LogFilesParser)
                            (parser as LogFilesParser).ProcessToParseAllFolder();
                    break;
                case UpdateStatisticWorkerMode.ParseSpecificFolder:
                    foreach (var parser in _fileParsers)
                        if (parser is LogFilesParser)
                            (parser as LogFilesParser).ProcessToParseSpecificFolder(SpecificFolderPath);
                    break;
                case UpdateStatisticWorkerMode.ParseNFolders:
                    foreach (var parser in _fileParsers)
                        if (parser is LogFilesParser)
                            (parser as LogFilesParser).ProcessToParseNFolders(NumberOfDays);
                    break;
                case UpdateStatisticWorkerMode.GetUsrFiles:
                    foreach (var parser in _fileParsers)
                        if (parser is LogFilesParser)
                            (parser as LogFilesParser).ProcessToGetUsrFiles();
                    break;
                case UpdateStatisticWorkerMode.UpdateInfoFromCommonLogFile:
                    foreach (var parser in _fileParsers)
                        if (parser is CommonLogFilesParser)
                            (parser as CommonLogFilesParser).ProcessToParseSpecificFile(SpecificFilePath);
                    break;
                case UpdateStatisticWorkerMode.Recovery:
                    foreach (var parser in _fileParsers)
                        if (parser is LogFilesParser)
                            (parser as LogFilesParser).ProcessToRecoverMissedMessages();
                    break;
                case UpdateStatisticWorkerMode.Repair:
                    foreach (var parser in _fileParsers)
                    {
                        if (parser is LogFilesParser)
                            (parser as LogFilesParser).ProcessToRepairUpdates();
                        if (parser is CommonLogFilesParser)
                            (parser as CommonLogFilesParser).ProcessToRepairUpdates();
                    }
                    break;
                default:
                    foreach (var parser in _fileParsers)
                        parser.StartWatching();
                    break;
            }
        }

        
    }
}
