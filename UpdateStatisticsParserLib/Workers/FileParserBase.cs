using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UpdateStatisticsParserLib.Mailer;
using UpdateStatisticsParserLib.Models;
using UpdateStatisticsParserLib.Providers;

namespace UpdateStatisticsParserLib.Workers
{
    public class FileParserBase
    {
        public DBProvider dbProvider;

        public List<string> LogFilesFolders { get; set; }
        public string LogFilesMask { get; set; }

        private static Logger logger = LogManager.GetLogger("Common log");

        public enum UpdateStatisticFileType
        {
            Log,
            Result,
            Common
        }

        protected virtual void OnWatchInitiated() { }

        public FileParserBase()
        {
            dbProvider = new DBProvider();
            LogFilesFolders = new List<string>();
        }
        /// <summary>
        /// Начать следить за изменениями в файлах логов
        /// </summary>
        public void StartWatching()
        {
            OnWatchInitiated();
            foreach (var workFolder in LogFilesFolders)
            {
                logger.Info(@"Начинаем следить за изменением файлов в папке: {0}", workFolder);

                if (!Directory.Exists(workFolder))
                    Directory.CreateDirectory(workFolder);

                FileSystemWatcher fsw = new FileSystemWatcher(workFolder, LogFilesMask);

                fsw.NotifyFilter = NotifyFilters.Size;
                fsw.Changed += new FileSystemEventHandler(onChanged);
                fsw.EnableRaisingEvents = true;

            }
            UpdateStatisticsWarningMailer.Instance.SendWarningTimer.Start();
        }

        public void onChanged(object sender, FileSystemEventArgs e)
        {
            UpdateStatisticsWarningMailer.Instance.SendWarningTimer.Stop();
            UpdateStatisticsWarningMailer.Instance.SendWarningTimer.Start();
            logger.Info(@"Изменение зафиксировано в файле: {0}", e.FullPath);
            OnPrepareParsing(e.FullPath);
        }

        protected virtual void OnPrepareParsing(string filePath) { }
        /// <summary>
        /// Начало парсинга файла
        /// </summary>
        /// <param name="filePath">Путь до файла</param>
        /// <param name="fileType">Тип файла</param>
        public void StartParsing(string filePath, UpdateStatisticFileType fileType)
        {
            FileInfo parseFile = new FileInfo(filePath);

            string directoryName = parseFile.Directory.Name;

            string workFolderName = new DirectoryInfo(parseFile.FullName).Parent.Parent.FullName;

            UpdateFile updateFile = dbProvider.GetUpdateFile(this, parseFile.Name, directoryName, parseFile);

            string[] lines = GetLines(filePath);
            int lastReadPosition = lines.Length-1;
            int newLinesCount = lines.Length - updateFile.RowsCount;
            lines = lines.Skip(updateFile.RowsCount).Take(newLinesCount).ToArray();

            OnStartParsing(lines, fileType, updateFile, directoryName, lastReadPosition, newLinesCount);
        }

        protected virtual void OnStartParsing(string[] lines, UpdateStatisticFileType fileType, UpdateFile updateFile, string directoryName, int lastReadPosition, int newLinesCount) { }
        /// <summary>
        /// Получить строки из файла
        /// </summary>
        /// <param name="fullPath">Полный путь до файла</param>
        /// <returns>Необработанные строки</returns>
        public string[] GetLines(string fullPath)
        {
            byte[] buffer;
            FileStream fileStream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            try
            {
                int length = (int)fileStream.Length;
                buffer = new byte[length];
                int count;
                int sum = 0;

                while ((count = fileStream.Read(buffer, sum, length - sum)) > 0)
                    sum += count;
            }
            finally
            {
                fileStream.Close();
            }
            string result = Encoding.Default.GetString(buffer);
            string[] lines = result.TrimEnd().Split('\n');

            return lines;
        }
    }
}
