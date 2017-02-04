using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UpdateStatisticsParserLib.Workers
{
    public partial class LogFilesParser
    {
        /// <summary>
        /// Метод для получения имени USR файла по определенному шаблону. 
        /// </summary>
        /// <param name="fileName">Имя лог файла</param>
        /// <param name="directoryName">Название директории, где находится лог файл</param>
        /// <param name="fileInstance">Экземпляр файла</param>
        private void TryToGetUsrFileName(string fileName, string directoryName, FileInfo fileInstance)
        {
            try
            {
                DirectoryInfo reportsDir = new DirectoryInfo(new DirectoryInfo(fileInstance.FullName).Parent.Parent.Parent.FullName + @"\Reports");

                string reportFileTemplate = string.Format(@"CONS#{0}#{1}", fileName.Split('#')[0], directoryName);
                logger.Info(@"Начинается поиск файла с шаблоном {0}...zip в директории {1}", reportFileTemplate, reportsDir.FullName);
               
                FileInfo reportFile = reportsDir.GetFiles().Where(fi => fi.Name.StartsWith(reportFileTemplate) && fi.Extension == ".zip").OrderByDescending(fi => fi.LastWriteTime).FirstOrDefault();

                if (reportFile != null)
                {
                    logger.Info(@"Файл найден. Информация в БД занесена");

                    dbProvider.AddReportFileInfo(reportFile.Name, fileName, directoryName);
                }
                else
                    logger.Info(@"Файл не найден.");
            }
            catch(Exception ex)
            {
                logger.Error(@"Ошибка при попытке найти USR файл. FileName: {0}, DirectoryName: {1}. Текст ошибки: {2}", fileName, directoryName, ex.Message);
            }
        }
    }
}
