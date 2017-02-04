using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace UpdateStatisticsParser
{
    class Program
    {
        private static bool stayActive = true;

        private static Dictionary<string, string> maintainceCommands = new Dictionary<string, string>
        {
            {"-h", "вывод этого сообщения" },
            {"-a [n]", "запуск в режиме парсинга всей директории Logs, либо только папок за последние n дней. Параметр n - опциональный" },
            {"-s <log_folder_path>", @"запуск в режиме парсинга конкретной папки с логами. Например -s C:\UpdateStat\Logs\1900-01-01" },
            {"-u", "запуск в режиме поиска USR файлов попыток пополнения" },
            {"-c <log_file_path>", @"запуск в режиме парсинга общего лога пополнений (файл с маской clientstat*.csv). Например -c C:\UpdateStat\Stat\clientstat_1900_01_01.csv" },
            {"-rec", @"запуск в режиме восстановления пропущенных записей" },
            {"-rep", @"запуск в режиме восстановления неполных записей о попытках пополнения"}
        };

        static void Main(string[] args)
        {
            Console.Clear();
            string[] userChoice = null;
            if (!args.Any())
            {
                Console.WriteLine("Программа для парсинга логов Сервера Интернет Пополнений. Выберите режим работы:");
                Console.WriteLine();

                while (userChoice == null || userChoice.Length == 0 || (!userChoice.Contains("/o") && !userChoice.Contains("/m")))
                {
                    Console.WriteLine("/o - онлайн режим работы приложения");
                    Console.WriteLine("/m - режим обслуживания и устранения неполадок");

                    userChoice = Console.ReadLine().Split(' ');
                }
            }
            else
            {
                userChoice = args;
                stayActive = false;
            }

            if (userChoice.Contains("/o"))
            {
                UpdateStatisticsParserLib.UpdateStatisticsParserWorker worker = new UpdateStatisticsParserLib.UpdateStatisticsParserWorker();
                worker.Start();
                stayActive = true;
            }
            else
            {
                if (userChoice.Length > 1)
                    RunProgram(userChoice.Skip(1).ToArray());
                else
                    PrintMaintainceCommands();
            }

            if (stayActive)
                Console.ReadLine();
        }

        private static void PrintMaintainceCommands()
        {
            Console.WriteLine("Введите комманду:");
            PrintCommands();
            string[] userChoice = Console.ReadLine().Split(' ');
            while (userChoice == null || userChoice.Length == 0 || !IsUserChoiceValid(userChoice) || userChoice[0] == "-h")
            {
                if (userChoice != null && userChoice[0] == "-h")
                    PrintCommands();
                else
                    Console.WriteLine("Нераспознанная команда.");
                userChoice = Console.ReadLine().Split(' ');
            }

            RunProgram(userChoice);
        }

        private static void RunProgram(string[] arguments)
        {
            UpdateStatisticsParserLib.UpdateStatisticsParserWorker worker = new UpdateStatisticsParserLib.UpdateStatisticsParserWorker();
            Console.Clear();
            switch (arguments[0])
            {
                case "-a":
                    if (arguments.Length > 1)
                    {
                        worker.UpdateStatWorkerMode = UpdateStatisticsParserLib.UpdateStatisticsParserWorker.UpdateStatisticWorkerMode.ParseNFolders;
                        int numberOfDays = 0;
                        if (int.TryParse(arguments[1], out numberOfDays))
                            worker.NumberOfDays = numberOfDays;
                        else
                        {
                            Console.WriteLine("Количество дней имеет неверный формат");
                            PrintMaintainceCommands();
                        }
                    }
                    else
                        worker.UpdateStatWorkerMode = UpdateStatisticsParserLib.UpdateStatisticsParserWorker.UpdateStatisticWorkerMode.ParseAllFolder;
                    break;
                case "-s":
                    worker.UpdateStatWorkerMode = UpdateStatisticsParserLib.UpdateStatisticsParserWorker.UpdateStatisticWorkerMode.ParseSpecificFolder;
                    worker.SpecificFolderPath = arguments[1];
                    break;
                case "-u":
                    worker.UpdateStatWorkerMode = UpdateStatisticsParserLib.UpdateStatisticsParserWorker.UpdateStatisticWorkerMode.GetUsrFiles;
                    break;
                case "-c":
                    worker.UpdateStatWorkerMode = UpdateStatisticsParserLib.UpdateStatisticsParserWorker.UpdateStatisticWorkerMode.UpdateInfoFromCommonLogFile;
                    worker.SpecificFilePath = arguments[1];
                    break;
                case "-rec":
                    worker.UpdateStatWorkerMode = UpdateStatisticsParserLib.UpdateStatisticsParserWorker.UpdateStatisticWorkerMode.Recovery;
                    break;
                case "-rep":
                    worker.UpdateStatWorkerMode = UpdateStatisticsParserLib.UpdateStatisticsParserWorker.UpdateStatisticWorkerMode.Repair;
                    break;
                default:
                    break;
            }
            worker.Start();
        }

        private static bool IsUserChoiceValid(string[] userChoice)
        {
            return maintainceCommands.Any(uc => uc.Key.Split(' ')[0] == userChoice[0]);
        }

        private static void PrintCommands()
        {
            Console.WriteLine("********************************************************");
            foreach (KeyValuePair<string, string> command in maintainceCommands)
            {
                Console.WriteLine("{0} - {1}", command.Key, command.Value);
            }
            Console.WriteLine("********************************************************");
        }
    }
}
