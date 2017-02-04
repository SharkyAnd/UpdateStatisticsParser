using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UpdateStatisticsParserLib.Models;

namespace UpdateStatisticsParserLib.Workers
{
    public partial class LogFilesParser : FileParserBase
    {
        private static Logger logger = LogManager.GetLogger("Log files parser");

        protected override void OnWatchInitiated()
        {
            LogFilesMask = "*.log";
            foreach (var workFolder in UpdateStatisticsParserConfig.Instance.LogFilesFolders)
            {
                for (int i = 0; i < 2; i++)
                {
                    string watchFolder = string.Format(@"{0}\{1}", workFolder.Value, DateTime.Now.AddDays(-i).ToString("yyyy_MM_dd"));
                    LogFilesFolders.Add(watchFolder);
                }
            }
        }
        /// <summary>
        /// Метод для получения названий USR файлов для попыток пополнения, у которых это значение не было заполнено
        /// </summary>
        public void ProcessToGetUsrFiles()
        {
            List<UpdateFile> updateFilesWithoutUsr = dbProvider.GetFilesWithoutUsr();

            foreach (UpdateFile updateFile in updateFilesWithoutUsr)
            {
                TryToGetUsrFileName(updateFile.FileName, updateFile.DirectoryName,
                    updateFile.FileInstance);
            }
        }
        /// <summary>
        /// Метод для обработки лог файлов попыток пополнения в конкретной директории
        /// </summary>
        /// <param name="specificFolderPath">Полный путь до папки</param>
        public void ProcessToParseSpecificFolder(string specificFolderPath)
        {
            string[] filePaths = Directory.GetFiles(specificFolderPath);

            LogManager.GetCurrentClassLogger().Info(@"Начинается обработка файла: {0}", specificFolderPath);
            LogManager.GetCurrentClassLogger().Info(@"Всего {0} файлов", filePaths.Length);
            for (int i = 0; i < filePaths.Length; i++)
            {
                LogManager.GetCurrentClassLogger().Info(@"Обработка {0} файла из {1}", i, filePaths.Length);
                OnPrepareParsing(filePaths[i]);
            }
        }
        /// <summary>
        /// Метод для обработки всех лог файлов
        /// </summary>
        public void ProcessToParseAllFolder()
        {
            foreach (var workFolder in UpdateStatisticsParserConfig.Instance.LogFilesFolders)
            {
                LogManager.GetCurrentClassLogger().Info(@"Начинается обработка файлов в папке: {0}", workFolder.Value);

                string[] files = Directory.GetFiles(workFolder.Value, "*.log", SearchOption.AllDirectories);

                LogManager.GetCurrentClassLogger().Info(@"Всего {0} файлов", files.Length);
                int i = 1;
                foreach (string filePath in files)
                {
                    LogManager.GetCurrentClassLogger().Info(@"Обработка {0} файла из {1}", i, files.Length);
                    OnPrepareParsing(filePath);
                    i++;
                }
            }
        }
        /// <summary>
        /// Метод для восстановления сообщений лог файлов, которые не были зафиксированы
        /// </summary>
        public void ProcessToRecoverMissedMessages()
        {
            LogManager.GetCurrentClassLogger().Info(@"Получаем файлы с незавершенным пополнением");
            List<UpdateFile> filesWithUnfinishedUpdate = dbProvider.GetFilesWithUnfinishedUpdate();
            LogManager.GetCurrentClassLogger().Info(@"Всего файлов: {0}", filesWithUnfinishedUpdate.Count);
            foreach (UpdateFile fileWithUnfinishedUpdate in filesWithUnfinishedUpdate)
            {
                LogManager.GetCurrentClassLogger().Info(@"Обрабатывается файл {0}. Директория: {1}", fileWithUnfinishedUpdate.FileName, fileWithUnfinishedUpdate.DirectoryName);
                OnPrepareParsing(fileWithUnfinishedUpdate.FileInstance.FullName);
            }
        }
        /// <summary>
        /// Метод для восстановления информации о попытках пополнения, которая не была записана
        /// </summary>
        public void ProcessToRepairUpdates()
        {
            LogManager.GetCurrentClassLogger().Info(@"Получаем файлы с некорректными данными о пополнении");
            LogManager.GetCurrentClassLogger().Info(@"Проверяются следующие критерии:\n-нет даты начала пополнения");
            List<ClientUpdate> brokenUpdates = dbProvider.GetBrokenUpdates();
            int length = brokenUpdates.Count;
            LogManager.GetCurrentClassLogger().Info(@"Всего записей: {0}", length);
            int i = 1;
            foreach (ClientUpdate brokenUpdate in brokenUpdates)
            {
                LogManager.GetCurrentClassLogger().Info(@"Обрабатывается пополнение {0} из {1}. Id: {2}. DistributiveId: {3}", i, length, brokenUpdate.Id, brokenUpdate.DistributiveId);
                DateTime? startDate = dbProvider.GetStartDateFromUpdateFile(brokenUpdate.SessionId);
                if (startDate.HasValue)
                {
                    LogManager.GetCurrentClassLogger().Info(@"Дата начала попытки найдена, производится обновление записи");
                    dbProvider.UpdateBrokenUpdateStartDate(brokenUpdate.Id, startDate);
                }
                else
                    LogManager.GetCurrentClassLogger().Info(@"Дата начала попытки не найдена");
                i++;
            }
        }
        /// <summary>
        /// Метод для парсинга лог файлов за последние n дней
        /// </summary>
        /// <param name="numberOfDays">Количество дней</param>
        public void ProcessToParseNFolders(int numberOfDays)
        {
            DateTime today = DateTime.Now.Date;
            DateTime endDate = DateTime.Now.AddDays(-numberOfDays).Date;

            while(endDate != today)
            {
                foreach (var workFolder in UpdateStatisticsParserConfig.Instance.LogFilesFolders)
                {
                    string folderPath = $"{workFolder.Value}\\{endDate.ToString("yyyy_MM_dd")}";

                    LogManager.GetCurrentClassLogger().Info(@"Начинается обработка файлов в папке: {0}", folderPath);

                    string[] files = Directory.GetFiles(folderPath, "*.log", SearchOption.AllDirectories);

                    LogManager.GetCurrentClassLogger().Info(@"Всего {0} файлов", files.Length);
                    int i = 1;
                    foreach (string filePath in files)
                    {
                        LogManager.GetCurrentClassLogger().Info(@"Обработка {0} файла из {1}", i, files.Length);
                        OnPrepareParsing(filePath);
                        i++;
                    }
                }

                endDate = endDate.AddDays(1);
            }
        }

        protected override void OnPrepareParsing(string filePath)
        {
            FileInfo parseFile = new FileInfo(filePath);
            string[] name = parseFile.Name.Split('#');
            if (name[1].Contains("_letter"))
                return;
            else if (name[1].Contains("_result"))
                StartParsing(filePath, UpdateStatisticFileType.Result);
            else
                StartParsing(filePath, UpdateStatisticFileType.Log);
        }

        protected override void OnStartParsing(string[] lines, UpdateStatisticFileType fileType, UpdateFile updateFile, string directoryName, int lastReadPosition, int newLinesCount)
        {
            logger.Info(@"Начинается обработка файла пополнения.");
            logger.Info(@"Новых строк в файле: {0}", newLinesCount);
            if (fileType == UpdateStatisticFileType.Result)
            {
                lines = lines.Skip(2).ToArray();
                ParseResultFileLines(lines, updateFile.Id);
            }
            else
                ParseLogFileLines(lines, updateFile, directoryName, lastReadPosition);
        }
        /// <summary>
        /// Парсинг строк лог файла
        /// </summary>
        /// <param name="lines">Массив строк</param>
        /// <param name="updateFile">Экземпляр класса, представляющий лог файл</param>
        /// <param name="directoryName">Название директории, где находится лог файл</param>
        /// <param name="lastReadPosition">Номер последней обработанной строки в файле</param>
        private void ParseLogFileLines(string[] lines, UpdateFile updateFile, string directoryName, int lastReadPosition)
        {
            logger.Info(@"Установлен тип файла: Лог пополнения клиента. Начинается обработка новых строк");

            dbProvider.UpdateFileLastReadPosition(this, updateFile.FileName, directoryName, lastReadPosition);
            foreach (string line in lines)
            {
                if (string.IsNullOrEmpty(line.Trim()))
                    continue;

                string message = null;
                string[] words = line.Split(' ');
                updateFile.SessionId = words[2];
                for (int i = 7; i < words.Count(); i++)
                {
                    message += words[i] + " ";
                }
                message = message.Trim();
                DateTime date = DateTime.Parse(string.Format("{0} {1}", words[0], words[1]));
                if (message.StartsWith("Сессия удалена"))
                {
                    TryToGetUsrFileName(updateFile.FileName, directoryName, updateFile.FileInstance);
                    dbProvider.UpdateFileReadStatus(updateFile.FileName, directoryName);
                }

                dbProvider.InsertNewMessage(date, message, updateFile.Id, updateFile.SessionId);
            }
        }
        /// <summary>
        /// Метод для парсинга файла с результирующим набором полученных QST файлов
        /// </summary>
        /// <param name="lines">Массив строк</param>
        /// <param name="fileId">Идентификатор файла в БД</param>
        private void ParseResultFileLines(string[] lines, long fileId)
        {
            logger.Info(@"Установлен тип файла: Результирующий набор полученных QST файлов. Начинается обработка новых строк");

            foreach (string line in lines)
            {
                string[] words = line.Split(';');
                string qstFileName = words[1];
                int qstCode = Convert.ToInt32(words[0]);

                dbProvider.AddQstFile(qstFileName, qstCode, fileId);
            }
        }
    }
}
