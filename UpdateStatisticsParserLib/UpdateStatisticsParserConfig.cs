using NLog;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using UpdateStatisticsParserLib.Models;

namespace UpdateStatisticsParserLib
{
    public sealed class UpdateStatisticsParserConfig
    {
        private static UpdateStatisticsParserConfig _instance;

        public UpdateStatisticsParserConfig() { }

        public static UpdateStatisticsParserConfig Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new UpdateStatisticsParserConfig();

                    Instance.ConnectionString = _instance.ReadFromFile();

                    FillServerProperties();
                    FillCommonProperties();
                    GetColumnsAssociations();

                }
                return _instance;
            }
        }
        /// <summary>
        /// Чтение строки подключения из файла
        /// </summary>
        /// <returns>Строка подключения</returns>
        private string ReadFromFile()
        {
            string connectionString = @"data source = (localhost); initial catalog = dev; Integrated Security=true";
            if (File.Exists(Environment.CurrentDirectory + "/connection_string.xml"))
            {
                try
                {
                    XDocument doc = XDocument.Load(Environment.CurrentDirectory + "/connection_string.xml");
                    foreach (XElement el in doc.Root.Elements())
                    {
                        switch (el.Name.ToString())
                        {
                            case "connectionString":
                                connectionString = el.Value;
                                break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить строку подключения к БД. Продолжение невозможно. Текст ошибки: {0}", ex.Message);
                    Environment.Exit(0);
                }
            }
            else
            {
                var doc = new XDocument(
                    new XElement("Settings",
                        new XElement("connectionString", connectionString)
                        ));
                doc.Save(Environment.CurrentDirectory + "/connection_string.xml");
            }

            return connectionString;
        }
        /// <summary>
        /// Заполнить настройки, зависимые от имени сервера
        /// </summary>
        private static void FillServerProperties()
        {
            string query = @"SELECT e.Name, s.Name AS ServerName, ebs.Path FROM updateserver.EnvironmentsByServers ebs
                            LEFT JOIN updateserver.Environments e ON ebs.EnvironmentId = e.id
                            LEFT JOIN updateserver.Servers s ON ebs.ServerId = s.Id";
            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection cn = new SqlConnection(Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(dt);

                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить параметры приложения из БД. Продолжение невозможно. Текст ошибки: {0}", ex.Message);
                Environment.Exit(0);
            }
            var settings = dt.AsEnumerable().Select(r => new
            {
                Name = r["Name"].ToString(),
                Server = r["ServerName"].ToString(),
                Path = r["Path"].ToString()
            });

            foreach (var setting in settings)
            {
                switch (setting.Name)
                {
                    case "LogFilesFolder":
                        Instance.LogFilesFolders.Add(setting.Server, setting.Path);
                        break;
                    case "CommonLogFolder":
                        Instance.CommonLogFilesFolders.Add(setting.Server, setting.Path);
                        break;
                    default:
                        break;
                }
            }
        }
        /// <summary>
        /// Заполнить общие настройки
        /// </summary>
        private static void FillCommonProperties()
        {
            string query = @"SELECT Name, CommonValue FROM updateserver.Environments";

            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection cn = new SqlConnection(Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(dt);

                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить параметры приложения из БД. Продолжение невозможно. Текст ошибки: {0}", ex.Message);
                Environment.Exit(0);
            }

            var settings = dt.AsEnumerable().Select(r => new
            {
                Name = r["Name"].ToString(),
                Value = r["CommonValue"].ToString()
            });
            Instance.ReportMailTo = new List<string>();
            foreach (var setting in settings)
            {
                switch (setting.Name)
                {
                    case "CommonLogFileMask":
                        Instance.CommonLogFileMask = setting.Value;
                        break;
                    case "ReportServer":
                        Instance.ReportServer = setting.Value;
                        break;
                    case "ReportMailFrom":
                        Instance.ReportMailFrom = setting.Value;
                        break;
                    case "ReportMailTo":
                        Instance.ReportMailTo.Add(setting.Value);
                        break;
                    case "ReportSendTimeInMinutes":
                        Instance.ReportSendTimeInMinutes = Convert.ToDouble(setting.Value);
                        break;
                    default:
                        break;
                }
            }
        }

        /// <summary>
        /// Получить соответствия колонок в csv файле СИП колонкам в БД
        /// </summary>
        private static void GetColumnsAssociations()
        {
            string query = @"SELECT column_name, log_file_column_number FROM updateserver.ParserColumnOptions";

            DataTable dt = new DataTable();
            try
            {
                using (SqlConnection cn = new SqlConnection(Instance.ConnectionString))
                using (SqlCommand cmd = new SqlCommand(query, cn))
                {
                    cn.Open();
                    SqlDataAdapter da = new SqlDataAdapter(cmd);
                    da.Fill(dt);

                    cn.Close();
                }
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке получить параметры приложения из БД. Продолжение невозможно. Текст ошибки: {0}", ex.Message);
                Environment.Exit(0);
            }

            Instance.ColumnsToParse = dt.AsEnumerable().Select(r => new ColumnToParse
            {
                columnName = r["column_name"].ToString(),
                logFileColumnNumber = Convert.ToInt32(r["log_file_column_number"])
            }).ToList();
        }

        private string _connectionString;
        public string ConnectionString
        {
            get { return _connectionString; }
            set { _connectionString = value; }
        }

        #region ParseLogFiles
        private Dictionary<string, string> _logFilesFolders = new Dictionary<string, string> {  };
        public Dictionary<string, string> LogFilesFolders
        {
            get { return _logFilesFolders; }
            set { _logFilesFolders = value; }
        }
        #endregion

        #region Update Attempt Update Info
        private Dictionary<string, string> _commonLogFilesFolders = new Dictionary<string, string> {  };
        public Dictionary<string, string> CommonLogFilesFolders
        {
            get { return _commonLogFilesFolders; }
            set { _commonLogFilesFolders = value; }
        }

        private string _commonLogFileMask = "clientstat*.csv";
        public string CommonLogFileMask
        {
            get { return _commonLogFileMask; }
            set { _commonLogFileMask = value; }
        }

        private List<ColumnToParse> _columnsToParse = new List<ColumnToParse>
        {
            new ColumnToParse
                {
                    columnName = "system_code",
                    logFileColumnNumber = 1
                },
            new ColumnToParse
                {
                    columnName = "distr_number",
                    logFileColumnNumber = 2
                },
            new ColumnToParse
                {
                    columnName = "computer",
                    logFileColumnNumber = 3
                },
                new ColumnToParse
                {
                    columnName = "ip_address",
                    logFileColumnNumber = 4
                },
                new ColumnToParse
                {
                    columnName = "session_id",
                    logFileColumnNumber = 5
                },
                new ColumnToParse
                {
                    columnName = "start_date",
                    logFileColumnNumber = 6
                },
                new ColumnToParse
                {
                    columnName = "qst_recieved_time",
                    logFileColumnNumber = 8
                },
                new ColumnToParse
                {
                    columnName = "qst_recieved_size",
                    logFileColumnNumber = 9
                },
                new ColumnToParse
                {
                    columnName = "update_create_time",
                    logFileColumnNumber = 10
                },
                new ColumnToParse
                {
                    columnName = "update_size",
                    logFileColumnNumber = 11
                },
                new ColumnToParse
                {
                    columnName = "update_size_cache",
                    logFileColumnNumber = 13
                },
                new ColumnToParse
                {
                    columnName = "download_time",
                    logFileColumnNumber = 14
                },
                new ColumnToParse
                {
                    columnName = "update_time",
                    logFileColumnNumber = 15
                },
                new ColumnToParse
                {
                    columnName = "report_recieved_size",
                    logFileColumnNumber = 17
                },
                new ColumnToParse
                {
                    columnName = "end_date",
                    logFileColumnNumber = 18
                },
                new ColumnToParse
                {
                    columnName = "log_files_folder",
                    logFileColumnNumber = 21
                },
                new ColumnToParse
                {
                    columnName = "log_file_name",
                    logFileColumnNumber = 22
                },
                new ColumnToParse
                {
                    columnName = "result_log_file_name",
                    logFileColumnNumber = 23
                },
                new ColumnToParse
                {
                    columnName = "error_log_file_name",
                    logFileColumnNumber = 24
                },
                new ColumnToParse
                {
                    columnName = "usr_rar_file_name",
                    logFileColumnNumber = 25
                },
                new ColumnToParse
                {
                    columnName = "client_returned_code",
                    logFileColumnNumber = 26
                },
                new ColumnToParse
                {
                    columnName = "server_returned_code",
                    logFileColumnNumber = 27
                },
                new ColumnToParse
                {
                    columnName = "update_launch_method",
                    logFileColumnNumber = 28
                },
                new ColumnToParse
                {
                    columnName = "res_version",
                    logFileColumnNumber = 29
                },
                new ColumnToParse
                {
                    columnName = "download_speed",
                    logFileColumnNumber = 30
                },
                new ColumnToParse
                {
                    columnName = "send_anon_tech_info",
                    logFileColumnNumber = 31
                },
                new ColumnToParse
                {
                    columnName = "send_stt",
                    logFileColumnNumber = 32
                },
                new ColumnToParse
                {
                    columnName = "inet_ext_key",
                    logFileColumnNumber = 33
                }
        };

        public List<ColumnToParse> ColumnsToParse
        {
            get { return _columnsToParse; }
            set { _columnsToParse = value; }
        }

        #endregion

        #region Mailer
        private string _reportServer = "post";
        public string ReportServer
        {
            get { return _reportServer; }
            set { _reportServer = value; }
        }

        private string _reportMailFrom = "report@cons-sakh.ru";
        public string ReportMailFrom
        {
            get { return _reportMailFrom; }
            set { _reportMailFrom = value; }
        }

        private List<string> _reportMailTo = new List<string> { "pr@cons-sakh.ru"/*, "cio@cons-sakh.ru", "pito@cons-sakh.ru"*/ };
        public List<string> ReportMailTo
        {
            get { return _reportMailTo; }
            set { _reportMailTo = value; }
        }
        private double _reportSendTimeInMinutes = 1;
        public double ReportSendTimeInMinutes
        {
            get { return _reportSendTimeInMinutes; }
            set { _reportSendTimeInMinutes = value; }
        }
        #endregion
    }
}
