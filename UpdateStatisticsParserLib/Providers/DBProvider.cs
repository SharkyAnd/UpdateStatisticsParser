using System;
using System.Data;
using System.Data.SqlClient;
using UpdateStatisticsParserLib.Models;
using UpdateStatisticsParserLib.Helpers;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using UpdateStatisticsParserLib.Workers;
using System.ComponentModel;
using NLog;

namespace UpdateStatisticsParserLib.Providers
{
    public class DBProvider
    {
        #region LogFiles
        /// <summary>
        /// Метод для добавления нового сообщения из лог файла в БД
        /// </summary>
        /// <param name="date">Дата сообщения</param>
        /// <param name="message">Текст сообщения</param>
        /// <param name="updateFileId">Идентификатор файла</param>
        /// <param name="sessionId">Идентификатор сессии</param>
        internal void InsertNewMessage(DateTime date, string message, long updateFileId, string sessionId)
        {
            string query = string.Empty;
            bool recordExist = DBCheckRecordExisting("UpdateAttemptFilesMessages", new Dictionary<string, object>
            {
                { "Date", date},
                { "Message", message},
                { "UpdateFileId", updateFileId}
            });

            if (recordExist)
                return;

            query = "INSERT INTO updateserver.UpdateAttemptFilesMessages (UpdateFileId, Message, Date, SessionId) " +
                   "VALUES (@UpdateFileId, @Message, @Date, @SessionId) ";

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cmd.Parameters.Add("@Message", SqlDbType.VarChar, Int32.MaxValue).Value = message;
                    cmd.Parameters.Add("@UpdateFileId", SqlDbType.BigInt).Value = updateFileId;
                    cmd.Parameters.Add("@Date", SqlDbType.DateTime).Value = date;
                    cmd.Parameters.Add("@SessionId", SqlDbType.VarChar, 250).Value = sessionId;

                    cn.Open();
                    cmd.ExecuteNonQuery();
                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger("Log files parser").Error("Ошибка при попытке добавить новое сообщения из файла пополнения. UpdateFileId: {0}. Текст ошибки: {1}", updateFileId, ex.Message);
            }
        }
        /// <summary>
        /// Метод для добавления новой строки с информацией об обработанном QST файле в БД
        /// </summary>
        /// <param name="qstFileName">Имя QST файла</param>
        /// <param name="qstCode">Код, определяющий результат обработки</param>
        /// <param name="updateFileId">Идентификатор файла</param>
        internal void AddQstFile(string qstFileName, int qstCode, long updateFileId)
        {
            bool recordExisting = DBCheckRecordExisting("UpdateAttemptQstFiles", new Dictionary<string, object>
            {
                { "QstFileName", qstFileName},
                { "StatusCode", qstCode},
                { "UpdateFileId", updateFileId}
            });

            if (recordExisting)
                return;

            string query = "INSERT INTO updateserver.UpdateAttemptQstFiles (QstFileName, StatusCode, UpdateFileId) " +
                   "VALUES (@QstFileName, @StatusCode, @UpdateFileId) ";

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cmd.Parameters.Add("@QstFileName", SqlDbType.VarChar, Int32.MaxValue).Value = qstFileName;
                    cmd.Parameters.Add("@UpdateFileId", SqlDbType.BigInt).Value = updateFileId;
                    cmd.Parameters.Add("@StatusCode", SqlDbType.Int).Value = qstCode;

                    cn.Open();
                    cmd.ExecuteNonQuery();
                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger("Log files parser").Error("Ошибка при попытке добавить новый QST файл в БД. QstFileName: {0}. Текст ошибки: {1}", qstFileName, ex.Message);
            }
        }
        /// <summary>
        /// Метод для получение идентификатора дистрибутива из БД по его номеру
        /// </summary>
        /// <param name="distrNumber">Номер дистрибутива</param>
        /// <returns>Идентификатор дистрибутива</returns>
        internal int? GetRightDistributiveId(string distrNumber)
        {
            int? distributiveId = null;

            string query2 = @"SELECT Id, SoprType, CASE WHEN PodklDate IS NULL THEN InstDate ELSE PodklDate END AS PodklDate FROM Distributives WHERE Number = @DistrNumber";

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query2, cn))
                {
                    cmd.Parameters.Add("@DistrNumber", SqlDbType.VarChar, Int32.MaxValue).Value = distrNumber;
                    cn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataTable dt = new DataTable();
                    da.Fill(dt);

                    var distributives = dt.AsEnumerable().Select(r => new
                    {
                        Id = (int?)r["Id"],
                        SoprType = r["SoprType"] == DBNull.Value ? null : r["SoprType"].ToString(),
                        PodklDate = r["PodklDate"] == DBNull.Value ? (DateTime?)null : Convert.ToDateTime(r["PodklDate"])
                    }).ToList();

                    distributiveId = distributives.Where(d => d.SoprType == "+").Select(d => d.Id).FirstOrDefault();

                    if (!distributiveId.HasValue)
                        distributiveId = distributives.OrderByDescending(d => d.PodklDate).Select(d => d.Id).FirstOrDefault();

                    cn.Close();
                    da.Dispose();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить дистрибутив из БД. DistrNumber: {0}. Текст ошибки: {1}", distrNumber, ex.Message);
            }
            return distributiveId;
        }
        /// <summary>
        /// Метод для обновления статуса, отвечающего за завершения обновления
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="fileName"></param>
        /// <param name="directoryName"></param>
        internal void UpdateFileReadStatus(string fileName, string directoryName)
        {
            string query = @"UPDATE updateserver.UpdateAttemptFiles SET UpdateFinished = 1 WHERE FileName = @FileName AND DirectoryName = @DirectoryName";

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cmd.Parameters.Add("@FileName", SqlDbType.VarChar, Int32.MaxValue).Value = fileName;
                    cmd.Parameters.Add("@DirectoryName", SqlDbType.VarChar, Int32.MaxValue).Value = directoryName;

                    cn.Open();
                    cmd.ExecuteNonQuery();
                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке обновить статус файла пополнения. FileName: {0}, DirectoryName: {1}. Текст ошибки: {2}", fileName, directoryName, ex.Message);
            }
        }

        #endregion

        #region Repair Broken Messages
        /// <summary>
        /// Метод для получения попыток пополнения с незаполненной информацией
        /// </summary>
        /// <returns>Список попыток пополнения</returns>
        internal List<ClientUpdate> GetBrokenUpdates()
        {
            List<ClientUpdate> clientUpdates = new List<ClientUpdate>();

            //2016-25-06 - дата запуска нового СИП
            //позже этой даты нет смысла искать записи

            string query = @"SELECT id, session_id, iu_client_distr_id FROM updateserver.ClientStatistic WHERE start_date IS NULL AND end_date >= '2016-25-06'";
            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataTable dt = new DataTable();
                    da.Fill(dt);

                    clientUpdates = dt.AsEnumerable().Select(r =>
                    {
                        var clientUpdate = new ClientUpdate
                        {
                            Id = (int?)r["Id"],
                            SessionId = r["session_id"] == DBNull.Value ? null : r["session_id"].ToString(),
                            DistributiveId = r["iu_client_distr_id"] == DBNull.Value ? (int?)null : Convert.ToInt32(r["iu_client_distr_id"])
                        };
                        return clientUpdate;
                    }).ToList();

                    cn.Close();
                    da.Dispose();
                }
                return clientUpdates;
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить некорректные записи о пополнениях из БД. Текст ошибки: {0}", ex.Message);
                return null;
            }
        }
        /// <summary>
        /// Метод для обновления даты начала пополнения
        /// </summary>
        /// <param name="brokenUpdateId">Идентификатор попытки пополнения</param>
        /// <param name="startDate">Дата начала пополнения</param>
        internal void UpdateBrokenUpdateStartDate(int? brokenUpdateId, DateTime? startDate)
        {
            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(@"UPDATE updateserver.ClientStatistic SET start_date = @StartDate WHERE id = @UpdateId", cn))
                {
                    cn.Open();

                    cmd.Parameters.AddWithValue("@StartDate", startDate);
                    cmd.Parameters.AddWithValue("@UpdateId", brokenUpdateId);

                    cmd.ExecuteNonQuery();

                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить некорректные записи о пополнениях из БД. Текст ошибки: {0}", ex.Message);
            }
        }

        #endregion

        #region Recover Missed Messages
        /// <summary>
        /// Метод для получения попыток пополнения с пропущенными записями
        /// </summary>
        /// <returns>Список попыток пополнения</returns>
        internal List<UpdateFile> GetFilesWithUnfinishedUpdate()
        {
            List<UpdateFile> updateFiles = new List<UpdateFile>();
            string query = @"SELECT uaf.ID, uaf.FileName, uaf.DirectoryName, s.Name AS ServerName
                            FROM updateserver.UpdateAttemptFiles uaf 
                            LEFT JOIN updateserver.Servers s ON uaf.ServerId = s.id
                            WHERE uaf.UpdateFinished = 0 AND uaf.FileName NOT LIKE '%_result.log'";
            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataTable dt = new DataTable();
                    da.Fill(dt);

                    updateFiles = dt.AsEnumerable().Select(r =>
                    {
                        var updFile = new UpdateFile
                        {
                            Id = (long)r["Id"],
                            FileName = r["FileName"] == DBNull.Value ? null : r["FileName"].ToString(),
                            DirectoryName = r["DirectoryName"] == DBNull.Value ? null : r["DirectoryName"].ToString(),
                            ServerName = r["ServerName"] == DBNull.Value ? null : r["ServerName"].ToString()
                        };
                        updFile.FileInstance = new FileInfo(string.Format(@"{0}\{1}\{2}",
                            UpdateStatisticsParserConfig.Instance.LogFilesFolders.Where(lff => lff.Key == updFile.ServerName).Select(lff => lff.Value).FirstOrDefault(),
                            updFile.DirectoryName,
                            updFile.FileName));
                        return updFile;
                    }).ToList();

                    cn.Close();
                    da.Dispose();
                }
                return updateFiles;
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить файлы из БД для восстановления истории сообщений. Текст ошибки: {0}", ex.Message);
                return null;
            }
        }
        #endregion

        #region Report File
        /// <summary>
        /// Метод для добавления в БД информации об архиве с USR файлом
        /// </summary>
        /// <param name="rarFilename">Название архива</param>
        /// <param name="fileName">Имя файла попытки пополнения</param>
        /// <param name="directoryName">Название директории, где находится лог файл с попыткой пополнения</param>
        internal void AddReportFileInfo(string rarFilename, string fileName, string directoryName)
        {
            string query = @"UPDATE updateserver.UpdateAttemptFiles SET UsrRarFileName = @RarFileName WHERE FileName = @FileName AND DirectoryName = @DirectoryName";

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cmd.Parameters.Add("@RarFileName", SqlDbType.VarChar, 250).Value = rarFilename;
                    cmd.Parameters.Add("@FileName", SqlDbType.VarChar, Int32.MaxValue).Value = fileName;
                    cmd.Parameters.Add("@DirectoryName", SqlDbType.VarChar, Int32.MaxValue).Value = directoryName;

                    cn.Open();
                    cmd.ExecuteNonQuery();
                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке добавить информацию об архиве с USR файлом. FileName: {0}, DirectoryName: {1}. Текст ошибки: {2}", fileName, directoryName, ex.Message);
            }
        }

        /// <summary>
        /// Метод для получения попыток пополнения с незаполненной информации о названии архива с USR файлом
        /// </summary>
        /// <returns></returns>
        internal List<UpdateFile> GetFilesWithoutUsr()
        {
            List<UpdateFile> updateFiles = new List<UpdateFile>();
            string query2 = @"SELECT uaf.Id, uaf.FileName, uaf.DirectoryName, s.Name AS ServerName 
                                FROM updateserver.UpdateAttemptFiles uaf 
                                LEFT JOIN updateserver.Servers s ON uaf.ServerId = s.id
                                WHERE UsrRarFileName IS NULL AND UpdateFinished = 1";
            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query2, cn))
                {
                    cn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    DataTable dt = new DataTable();
                    da.Fill(dt);

                    updateFiles = dt.AsEnumerable().Select(r =>
                    {
                        var updFile = new UpdateFile
                        {
                            Id = (long)r["Id"],
                            FileName = r["FileName"] == DBNull.Value ? null : r["FileName"].ToString(),
                            DirectoryName = r["DirectoryName"] == DBNull.Value ? null : r["DirectoryName"].ToString(),
                            ServerName = r["ServerName"] == DBNull.Value ? null : r["ServerName"].ToString()
                        };
                        updFile.FileInstance = new FileInfo(string.Format(@"{0}\{1}\{2}",
                            UpdateStatisticsParserConfig.Instance.LogFilesFolders.Where(lff => lff.Key == updFile.ServerName).Select(lff => lff.Value).FirstOrDefault(),
                            updFile.DirectoryName,
                            updFile.FileName));
                        return updFile;
                    }).ToList();

                    cn.Close();
                    da.Dispose();
                }
                return updateFiles;
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить файлы без архивов USR из БД. Текст ошибки: {0}", ex.Message);
                return null;
            }
        }
        #endregion

        #region Update Common Statistic Info

        public List<ClientUpdate> GetCFBrokenUpdates()
        {
            List<ClientUpdate> brokenUpdates = new List<ClientUpdate>();

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("SELECT id, distr_number, computer FROM updateserver.ClientStatistic WHERE iu_client_distr_id IS NULL", cn))
                    {
                        cn.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        DataTable dt = new DataTable();
                        da.Fill(dt);

                        brokenUpdates = dt.AsEnumerable().Select(r =>
                        {
                            ClientUpdate upd = new ClientUpdate
                            {
                                Id = Convert.ToInt32(r["Id"])
                            };

                            string distrNumber = r["distr_number"].ToString();
                            string computer = r["computer"].ToString();
                            string computerNumber = (computer == "01" || string.IsNullOrEmpty(computer))? "" : "." + computer.NormalizeDistributive();
                            if (computerNumber.Length > 3)
                                computerNumber = null;

                            upd.DistributiveNumber = distrNumber.NormalizeDistributive() + computerNumber;

                            return upd;
                        }).ToList();

                        cn.Close();
                        da.Dispose();
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить попытки пополнения, у которых отсутствует DistributiveID. Message: {0}", ex.Message);
            }

            return brokenUpdates;
        }

        public void AddDistributiveIdToRecord(ClientUpdate record)
        {
            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("UPDATE updateserver.ClientStatistic SET iu_client_distr_id = @DistrID WHERE id = @RecordID", cn))
                    {
                        cn.Open();

                        cmd.Parameters.AddWithValue("@RecordID", record.Id);

                        int? distrId = GetRightDistributiveId(record.DistributiveNumber);

                        cmd.Parameters.AddWithValue("DistrID", distrId);

                        cmd.ExecuteNonQuery();

                        cn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке обновить DistributiveID записи. ID: {0}, DistributiveNumber: {1}. Message: {2}", record.Id, record.DistributiveNumber, ex.Message);
            }
        }

        /// <summary>
        /// Метод для добавления новой записи о попытке пополнения из общего файла со статистикой
        /// </summary>
        /// <param name="fileLine">Запись</param>
        /// <param name="fileInstance">Экземпляр файла со статистикой</param>
        internal void AddNewCommonFileRecord(string fileLine, FileInfo fileInstance)
        {
            string[] lineWords = fileLine.Split(';');

            DateTime? startDate = null;

            if (lineWords[6] == "0")
                startDate = GetStartDateFromUpdateFile(lineWords[5]);
            else
                startDate = Convert.ToDateTime(lineWords[6] + " " + lineWords[7]);

            bool recordExisting = DBCheckRecordExisting("ClientStatistic", new Dictionary<string, object>
                {
                    {"distr_number",  lineWords[2]},
                    {"session_id",lineWords[5] },
                    {"start_date",startDate }
                }
            );

            if (recordExisting)
            {
                LogManager.GetLogger("Common log file logger").Info("Запись уже существует в БД. № записи: {0}, DistrNumber: {1}, SessionId: {2}", lineWords[0], lineWords[2], lineWords[5]);
                return;
            }

            string insertQueryPart = "server_id";
            string valuesQueryPart = "(SELECT TOP 1 id FROM updateserver.Servers WHERE Name = @ServerName)";

            List<SqlParameter> parameters = new List<SqlParameter>();
            SqlParameter serverName = new SqlParameter("@ServerName", SqlDbType.VarChar);
            serverName.Value = UpdateStatisticsParserConfig.Instance.CommonLogFilesFolders.Where(clf => clf.Value == new DirectoryInfo(fileInstance.FullName).Parent.FullName).Select(clf => clf.Key).FirstOrDefault();
            parameters.Add(serverName);

            insertQueryPart += ", iu_client_distr_id";
            valuesQueryPart += ", @DistrId";
            string distrNumber = lineWords[2];
            string computerNumber = lineWords[3] == "01" ? "" : "." + lineWords[3].NormalizeDistributive();
            if (computerNumber.Length > 3)
                computerNumber = null;

            string dbDistrNumber = distrNumber.NormalizeDistributive() + computerNumber;

            int? distrId = GetRightDistributiveId(dbDistrNumber);

            SqlParameter distrIdP = new SqlParameter("@DistrId", SqlDbType.Int);
            distrIdP.Value = distrId.HasValue ? distrId : (object)DBNull.Value;
            parameters.Add(distrIdP);

            insertQueryPart += ", update_file_id";
            valuesQueryPart += ", @UpdateFileId";

            long? updateFileId = GetUpdateFileId(lineWords[5]);

            SqlParameter updateFileIdP = new SqlParameter("@UpdateFileId", SqlDbType.BigInt);
            updateFileIdP.Value = updateFileId.HasValue ? updateFileId : (object)DBNull.Value;
            parameters.Add(updateFileIdP);

            insertQueryPart += ", start_date";
            valuesQueryPart += ", @StartDate";

            SqlParameter StartDateP = new SqlParameter("@StartDate", SqlDbType.DateTime);
            StartDateP.Value = startDate.HasValue ? startDate : (object)DBNull.Value;
            parameters.Add(StartDateP);

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand("SET FMTONLY ON; select * from updateserver.ClientStatistic; SET FMTONLY OFF", cn))
                    {
                        cn.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        DataTable dt = new DataTable();
                        da.Fill(dt);
                        Dictionary<string, Type> types = new Dictionary<string, Type>();
                        foreach (DataColumn dtCol in dt.Columns)
                        {
                            if (dtCol.ColumnName == "start_date")
                                continue;
                            types.Add(dtCol.ColumnName, dtCol.DataType);
                        }

                        foreach (ColumnToParse column in UpdateStatisticsParserConfig.Instance.ColumnsToParse)
                        {

                            string unconvertedValue = null;
                            if (column.columnName == "end_date")
                            {
                                if (lineWords[column.logFileColumnNumber] == "0")
                                    unconvertedValue = null;
                                else
                                    unconvertedValue = lineWords[column.logFileColumnNumber] + " " + lineWords[column.logFileColumnNumber + 1];
                            }
                            else
                            {
                                unconvertedValue = lineWords[column.logFileColumnNumber];

                                if (unconvertedValue.Contains("TRUE"))
                                    unconvertedValue = "1";
                                if (unconvertedValue.Contains("FALSE"))
                                    unconvertedValue = "0";
                                if (unconvertedValue.Contains("NULL"))
                                    unconvertedValue = "2";
                            }

                            Type type = types.Where(t => t.Key == column.columnName).Select(t => t.Value).FirstOrDefault();
                            SqlDbType dbType = (SqlDbType)TypeMap.TypeMapper[type];
                            SqlParameter param = new SqlParameter(string.Format("@{0}", column.columnName), dbType);
                            if (unconvertedValue == null)
                            {
                                param.Value = DBNull.Value;
                                parameters.Add(param);
                            }
                            else
                            {
                                TypeConverter typeConverter = TypeDescriptor.GetConverter(type);
                                object convertedValue = typeConverter.ConvertFromString(unconvertedValue);

                                param.Value = convertedValue;
                                parameters.Add(param);
                            }
                            insertQueryPart += (insertQueryPart == null ? "" : ", ") + column.columnName;
                            valuesQueryPart += (valuesQueryPart == null ? "" : ", ") + string.Format("@{0}", column.columnName);
                        }

                        cn.Close();
                    }
                }
                string insertQuery = string.Format("INSERT INTO updateserver.ClientStatistic ({0}) VALUES ({1})", insertQueryPart, valuesQueryPart);
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(insertQuery, cn))
                    {
                        foreach (SqlParameter param in parameters)
                            cmd.Parameters.Add(param);

                        cn.Open();
                        cmd.ExecuteNonQuery();
                        LogManager.GetLogger("Common log file logger").Info(@"Запись № {0} добавлена", lineWords[0]);
                        cn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                LogManager.GetLogger("Common log file logger").Error("Ошибка при попытке добавить новую запись о пополнении. № строки: {0}, DistrNumber: {1}, SessionId: {2}. Текст ошибки: {3}", lineWords[0], lineWords[2], lineWords[5], ex.Message);
            }
        }
        /// <summary>
        /// Метод для получения даты начала попытки пополнения по идентификатору сессии
        /// </summary>
        /// <param name="sessionId">Идентификатор сессии</param>
        /// <returns>Дата начала пополнения</returns>
        internal DateTime? GetStartDateFromUpdateFile(string sessionId)
        {
            DateTime? startDate = null;

            string query = @"SELECT MIN(Date) FROM updateserver.UpdateAttemptFilesMessages WHERE SessionId = @SessionId HAVING MIN(Date) IS NOT NULL";

            using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cmd.Parameters.AddWithValue("@SessionId", sessionId);

                    cn.Open();

                    SqlDataReader reader = cmd.ExecuteReader();

                    if (reader.HasRows)
                        while (reader.Read())
                            startDate = reader.GetDateTime(0);

                    cn.Close();
                }
            }

            return startDate;
        }
        /// <summary>
        /// Метод для получения идентификатора попытки пополнения по идентификатору сессии
        /// </summary>
        /// <param name="sessionId">Идентификатор сессии</param>
        /// <returns>Идентификатор попытки пополнения</returns>
        private long? GetUpdateFileId(string sessionId)
        {
            long? updateFileId = null;

            string query = @"SELECT TOP 1 UpdateFileId FROM updateserver.UpdateAttemptFilesMessages WHERE SessionId = @SessionId";

            using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cmd.Parameters.AddWithValue("@SessionId", sessionId);

                    cn.Open();

                    SqlDataReader reader = cmd.ExecuteReader();

                    if (reader.HasRows)
                        while (reader.Read())
                            updateFileId = reader.GetInt64(0);

                    cn.Close();
                }
            }

            return updateFileId;
        }
        #endregion

        #region Common File Operations
        /// <summary>
        /// Метод для получения экземпляра попытки пополнения из БД
        /// </summary>
        /// <param name="sender">Класс, вызывающий метод</param>
        /// <param name="fileName">Имя лог файла попытки пополнения</param>
        /// <param name="directoryName">Название директории, где находится лог файл попытки пополнения</param>
        /// <param name="fileInstance">Экземпляр лог файла</param>
        /// <returns>Экземпляр попытки пополнения</returns>
        internal UpdateFile GetUpdateFile(object sender, string fileName, string directoryName, FileInfo fileInstance)
        {
            UpdateFile updateFile = new UpdateFile();
            updateFile.FileName = fileName;
            updateFile.DirectoryName = directoryName;
            updateFile.FileType = (sender is CommonLogFilesParser) ? FileType.Common : FileType.Log;
            updateFile.RowsCount = 0;
            updateFile.FileInstance = fileInstance;
            bool recordExist = true;
            string tableName = "UpdateAttemptFiles";
            if (sender is CommonLogFilesParser)
                tableName = "CommonLogFiles";

            string query = string.Format(@"SELECT Id, LastReadPosition FROM updateserver.{0} WHERE FileName = @FileName AND DirectoryName = @DirectoryName", tableName);

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                {
                    using (SqlCommand cmd = new SqlCommand(query, cn))
                    {
                        cmd.Parameters.Add("@FileName", SqlDbType.VarChar, Int32.MaxValue).Value = fileName;
                        cmd.Parameters.Add("@DirectoryName", SqlDbType.VarChar, Int32.MaxValue).Value = directoryName;

                        cn.Open();
                        SqlDataReader reader = cmd.ExecuteReader();

                        if (!reader.HasRows)
                            recordExist = false;
                        else
                        {
                            while (reader.Read())
                            {
                                updateFile.Id = Convert.ToInt64(reader[0]);
                                updateFile.RowsCount = Convert.ToInt32(reader[1]);
                            }
                        }

                    }
                    cn.Close();
                }

                if (!recordExist)
                    updateFile.Id = InsertNewUpdateFile(sender, updateFile);
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить файл пополнения из БД. FileName: {0}, DirectoryName: {1}. Текст ошибки: {2}", fileName, directoryName, ex.Message);
            }
            return updateFile;
        }
        /// <summary>
        /// Метод для добавления информации о новом лог файле в БД
        /// </summary>
        /// <param name="sender">Экземпляр класса, вызывающего метод</param>
        /// <param name="updateFile">Экземпляр попытки пополнения</param>
        /// <returns>Идентификатор новой записи</returns>
        internal long InsertNewUpdateFile(object sender, UpdateFile updateFile)
        {
            long Id = 0;
            using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
            {
                string watchServerName = null;
                string query = null;
                List<SqlParameter> sqlParameters = new List<SqlParameter>();

                SqlParameter fileNameP = new SqlParameter("@FileName", SqlDbType.VarChar, Int32.MaxValue); fileNameP.Value = updateFile.FileName; sqlParameters.Add(fileNameP);
                SqlParameter directoryNameP = new SqlParameter("@DirectoryName", SqlDbType.VarChar, Int32.MaxValue); directoryNameP.Value = updateFile.DirectoryName; sqlParameters.Add(directoryNameP);

                if (sender is CommonLogFilesParser)
                {
                    watchServerName = UpdateStatisticsParserConfig.Instance.CommonLogFilesFolders.Where(wk => wk.Value == new DirectoryInfo(updateFile.FileInstance.FullName).Parent.FullName).Select(wk => wk.Key).FirstOrDefault();
                    query = @"INSERT INTO updateserver.CommonLogFiles(FileName, DirectoryName, LastReadPosition, ServerId)
                              VALUES 
                              (
                                 @FileName, @DirectoryName, 
                                 0,
                                 (SELECT Id FROM updateserver.Servers WHERE Name = @ServerName)
                              )
                              SELECT @@IDENTITY";
                }
                else
                {
                    string[] fileNameParts = updateFile.FileName.Split('#')[0].Split('_');
                    watchServerName = UpdateStatisticsParserConfig.Instance.LogFilesFolders.Where(wk => wk.Value == new DirectoryInfo(updateFile.FileInstance.FullName).Parent.Parent.FullName).Select(wk => wk.Key).FirstOrDefault();

                    string distrNumber = fileNameParts[1];
                    string systemCode = fileNameParts[0];
                    string computerNumber = fileNameParts.Length > 2 ? "." + fileNameParts[2].NormalizeDistributive() : "";
                    if (computerNumber.Length > 3)
                        computerNumber = null;

                    string dbDistrNumber = null;

                    if (distrNumber.NormalizeDistributive() == "210" || computerNumber != ".1")
                        dbDistrNumber = distrNumber.NormalizeDistributive() + computerNumber;
                    else
                        dbDistrNumber = distrNumber.NormalizeDistributive();

                    LogManager.GetCurrentClassLogger().Debug("DistrNumber после нормализации: {0}", distrNumber);

                    int? distrId = GetRightDistributiveId(dbDistrNumber);

                    LogManager.GetCurrentClassLogger().Debug("DistrID: {0}", distrId.HasValue ? distrId.ToString() : "Нет");

                    query = @"UPDATE Distributives SET is_internet_update = 1 WHERE Number = @DistrNumber
                                          INSERT INTO updateserver.UpdateAttemptFiles(FileName, DirectoryName, DistrId, SystemCode, LastReadPosition, UpdateFinished, ServerId) 
                                          VALUES 
                                          (
                                            @FileName, @DirectoryName, 
                                            @DistrId,
                                            @SystemCode,
                                            0, 0,
                                            (SELECT Id FROM updateserver.Servers WHERE Name = @ServerName)
                                          )
                                         SELECT @@IDENTITY";

                    SqlParameter distrIdP = new SqlParameter("@DistrId", SqlDbType.Int); distrIdP.Value = distrId.HasValue ? distrId : (object)DBNull.Value; sqlParameters.Add(distrIdP);
                    SqlParameter systemCodeP = new SqlParameter("@SystemCode", SqlDbType.VarChar, Int32.MaxValue); systemCodeP.Value = systemCode; sqlParameters.Add(systemCodeP);
                    SqlParameter distrNumberP = new SqlParameter("@DistrNumber", SqlDbType.VarChar, Int32.MaxValue); distrNumberP.Value = dbDistrNumber; sqlParameters.Add(distrNumberP);
                }

                SqlParameter ServerNameP = new SqlParameter("@ServerName", SqlDbType.VarChar, Int32.MaxValue); ServerNameP.Value = watchServerName; sqlParameters.Add(ServerNameP);

                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    foreach (var param in sqlParameters)
                        cmd.Parameters.Add(param);

                    cn.Open();
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                        Id = Convert.ToInt64(reader[0]);
                }

                cn.Close();
            }

            return Id;
        }
        /// <summary>
        /// Метод для обновления номера последней обработанной строки в файле
        /// </summary>
        /// <param name="sender">Экземпляр класса, вызывающего метод</param>
        /// <param name="fileName">Имя файла</param>
        /// <param name="directoryName">Название директории, где находится файл</param>
        /// <param name="lastReadPosition">Номер последней обработанной строки в файле</param>
        internal void UpdateFileLastReadPosition(object sender, string fileName, string directoryName, int lastReadPosition)
        {
            string tableName = "UpdateAttemptFiles";
            if (sender is CommonLogFilesParser)
                tableName = "CommonLogFiles";
            string query = string.Format(@"UPDATE updateserver.{0} SET LastReadPosition = @LastReadPosition WHERE FileName = @FileName AND DirectoryName = @DirectoryName", tableName);

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cmd.Parameters.Add("@LastReadPosition", SqlDbType.Int).Value = lastReadPosition;
                    cmd.Parameters.Add("@FileName", SqlDbType.VarChar, Int32.MaxValue).Value = fileName;
                    cmd.Parameters.Add("@DirectoryName", SqlDbType.VarChar, Int32.MaxValue).Value = directoryName;

                    cn.Open();
                    cmd.ExecuteNonQuery();
                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке обновить последнюю позицию чтения файла пополнения. FileName: {0}, DirectoryName: {1}. Текст ошибки: {2}", fileName, directoryName, ex.Message);
            }
        }
        #endregion

        #region Database
        /// <summary>
        /// Метод для проверки дубликата записи в БД
        /// </summary>
        /// <param name="tableName">Имя таблицы в БД</param>
        /// <param name="parameters">Параметры запроса</param>
        /// <returns></returns>
        private bool DBCheckRecordExisting(string tableName, Dictionary<string, object> parameters)
        {
            bool recordExist = false;

            string query = string.Format(@"SELECT id FROM updateServer.{0} WHERE ",
                tableName);
            string whereClause = string.Empty;

            foreach (KeyValuePair<string, object> parameter in parameters)
                whereClause += string.IsNullOrEmpty(whereClause) ? string.Format(@"{0} = @{0}", parameter.Key) : string.Format(@" AND {0} = @{0}", parameter.Key);

            try
            {
                using (SqlConnection cn = new SqlConnection(UpdateStatisticsParserConfig.Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query + whereClause, cn))
                {
                    foreach (KeyValuePair<string, object> parameter in parameters)
                    {
                        SqlParameter param = new SqlParameter(parameter.Key, parameter.Value);
                        cmd.Parameters.Add(param);
                    }

                    cn.Open();

                    SqlDataReader reader = cmd.ExecuteReader();

                    if (reader.HasRows)
                        recordExist = true;

                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке проверить существование записи о пополнении. TableName: {0}. Текст ошибки: {1}", tableName, ex.Message);
            }

            return recordExist;
        }
        #endregion
    }
}
