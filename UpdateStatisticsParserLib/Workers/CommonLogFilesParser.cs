using NLog;
using System.IO;
using UpdateStatisticsParserLib.Models;
using System.Collections.Generic;

namespace UpdateStatisticsParserLib.Workers
{
    public partial class CommonLogFilesParser : FileParserBase
    {
        private static Logger logger = LogManager.GetLogger("Common log file logger");

        protected override void OnWatchInitiated()
        {
            LogFilesMask = UpdateStatisticsParserConfig.Instance.CommonLogFileMask;
            foreach (var workFolder in UpdateStatisticsParserConfig.Instance.CommonLogFilesFolders)
            {
                LogFilesFolders.Add(workFolder.Value);
            }
        }
        /// <summary>
        /// Метод для парсинга конкретного файла статистики СИП
        /// </summary>
        /// <param name="specificFilePath">Путь до файла</param>
        public void ProcessToParseSpecificFile(string specificFilePath)
        {
            if(string.IsNullOrEmpty(specificFilePath))
            {
                logger.Warn("Для файла не указан путь. Процесс завершен.");
                return;
            }
            FileInfo fileInsance = new FileInfo(specificFilePath);
            foreach (string line in GetLines(fileInsance.FullName))
            {
                if (string.IsNullOrEmpty(line.Trim()) || line.StartsWith("Номер записи"))
                    continue;
                dbProvider.AddNewCommonFileRecord(line, fileInsance);
            }
        }

        public void ProcessToRepairUpdates()
        {
            logger.Info(@"Получение попыток пополнения без DistributiveID");

            List<ClientUpdate> brokenUpdates = dbProvider.GetCFBrokenUpdates();
            int length = brokenUpdates.Count;
            logger.Info(@"Всего попыток {0}", length);
            int i = 1;
            foreach (ClientUpdate brokenUpdate in brokenUpdates)
            {
                logger.Info(@"Начинается обработка попытки {0} из {1}. Distributive Number: {2}",i, length, brokenUpdate.DistributiveNumber);

                brokenUpdate.DistributiveId = dbProvider.GetRightDistributiveId(brokenUpdate.DistributiveNumber);

                if(brokenUpdate.DistributiveId.HasValue)
                {
                    logger.Info(@"DistributiveId найден. Идет добавление в БД...");
                    dbProvider.AddDistributiveIdToRecord(brokenUpdate);
                }
                i++;
            }
        }

        protected override void OnPrepareParsing(string filePath)
        {
            FileInfo parseFile = new FileInfo(filePath);
            StartParsing(parseFile.FullName, UpdateStatisticFileType.Common);
        }

        protected override void OnStartParsing(string[] lines, UpdateStatisticFileType fileType, UpdateFile updateFile, string directoryName, int lastReadPosition, int newLinesCount)
        {
            logger.Info(@"Начинается обработка общего файла пополнения.");
            logger.Info(@"Новых строк в файле: {0}", newLinesCount);
            ParseCommonFileLines(lines, updateFile, directoryName, lastReadPosition);
        }
        /// <summary>
        /// Распарсить строки файла статистики СИП
        /// </summary>
        /// <param name="lines">Массив строк</param>
        /// <param name="updateFile">Экземпляр класса, представляющий файл статистики</param>
        /// <param name="directoryName">Название папки</param>
        /// <param name="lastReadPosition">Номер последней обработанной строки в файле</param>
        private void ParseCommonFileLines(string[] lines, UpdateFile updateFile, string directoryName, int lastReadPosition)
        {
            dbProvider.UpdateFileLastReadPosition(this, updateFile.FileName, directoryName, lastReadPosition);
            foreach (string line in lines)
            {
                if (string.IsNullOrEmpty(line.Trim()) || line.StartsWith("Номер записи"))
                    continue;

                dbProvider.AddNewCommonFileRecord(line, updateFile.FileInstance);
            }
        }     
    }
}
