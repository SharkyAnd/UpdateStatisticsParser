using NLog;
using System;
using System.Net.Mail;
using System.Timers;

namespace UpdateStatisticsParserLib.Mailer
{
    public sealed class UpdateStatisticsWarningMailer
    {
        private static UpdateStatisticsWarningMailer _instance;

        private static string MailFrom { get; set; }
        private static string ServerName { get; set; }
        private Timer _sendWarningTimer;
        public Timer SendWarningTimer { get { return _sendWarningTimer; } set { _sendWarningTimer = value; } }

        public static UpdateStatisticsWarningMailer Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new UpdateStatisticsWarningMailer();
                    ServerName = UpdateStatisticsParserConfig.Instance.ReportServer;
                    MailFrom = UpdateStatisticsParserConfig.Instance.ReportMailFrom;

                    _instance.SendWarningTimer = new Timer();
                    _instance.SendWarningTimer.Enabled = true;
                    _instance.SendWarningTimer.Interval = 1000 * 60 * UpdateStatisticsParserConfig.Instance.ReportSendTimeInMinutes;
                    _instance.SendWarningTimer.Elapsed += SendWarningTimer_Elapsed;
                }
                return _instance;
            }
        }

        private static void SendWarningTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            MailMessage mail = new MailMessage();
            SmtpClient client = new SmtpClient();
            try
            {
                mail.From = new MailAddress(MailFrom);
                foreach (var item in UpdateStatisticsParserConfig.Instance.ReportMailTo)
                {
                    mail.To.Add(new MailAddress(item));
                }
                mail.Subject = "Автоматически сгенерированное письмо приложения \"Статистика интернет пополнений\"";
                mail.Body = string.Format("К серверу интернет пополнения не было обращений более {0} минут. Рекомендуется проверить состояние сервера.", UpdateStatisticsParserConfig.Instance.ReportSendTimeInMinutes);

                client.Host = ServerName;
                client.Send(mail);
                mail.Dispose();
            }
            catch (Exception ex)
            {
                LogManager.GetCurrentClassLogger().Error("Ошибка при попытке отправить письмо с оповещением. Текст ошибки:{0}", ex.Message);
            }
            finally
            {
                mail.Dispose();
                client.Dispose();
            }
        }
    }
}
