using Microsoft.HBase.Client;
using Microsoft.HBase.Client.Filters;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SessionInfo
{
    static class Program
    {
        //base for Unix time
        private static DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        static void Main(string[] args)
        {
            
            //Connect to HBase
            var credentials = new ClusterCredentials(
                new Uri(Properties.Settings.Default.HBaseClusterUrl),
                Properties.Settings.Default.HBaseClusterUserName,
                Properties.Settings.Default.HBaseClusterPassword);
            //Get the client
            var hbaseClient = new HBaseClient(credentials);

            char operation = ' ';
            //Loop until 'q' is selected
            do {
                Console.Write("(C)reate table, (d)elete table, or search for (s)tart or (e)nd events. Or (q)uit: ");
                //Get the character
                operation = Console.ReadKey().KeyChar;
                Console.WriteLine();
                //Which operation?
                switch (operation)
                {
                    case 'c':
                        CreateHBaseTable(hbaseClient);
                        break;

                    case 'd':
                        DeleteHBaseTable(hbaseClient);
                        break;

                    case 's':
                        //Fall through since both Start and Event searches
                        //have the same logic/signature
                    case 'e':
                        DateTime start = GetDateTime("start");
                        DateTime end = GetDateTime("end");
                        //Was it start or end we want to look for?
                        string eventType = operation == 's' ? "START" : "END";
                        //Find sessions for the event type, by start and end times
                        GetSessionsByTime(hbaseClient, eventType, start, end);
                        break;

                    default:
                        break;
                }
            } while (operation != 'q');
        }

        /// <summary>
        /// Prompt the user for a DateTime value
        /// </summary>
        /// <param name="eventType">Event type, used to prompt the user</param>
        /// <returns>DateTime</returns>
        static DateTime GetDateTime(string eventType)
        {
            Console.Write("Enter the date/time to begin looking for {0} events: ", eventType);
            DateTime dateTime = Convert.ToDateTime(Console.ReadLine());
            return dateTime;
        }

        /// <summary>
        /// Create a new HBase table
        /// </summary>
        /// <param name="hbaseClient">client used to connect to HBase</param>
        static public void CreateHBaseTable(HBaseClient hbaseClient)
        {
            //Define the 'cf family
            //Set versions to retain to 5
            ColumnSchema cf = new ColumnSchema() {
                name = Properties.Settings.Default.HBaseTableColumnFamily,
                maxVersions = 5
            };
            //Define the table
            TableSchema tableSchema = new TableSchema();
            tableSchema.name = Properties.Settings.Default.HBaseTableName;
            tableSchema.columns.Add(cf);
            //Create the table
            hbaseClient.CreateTable(tableSchema);
            Console.WriteLine("Table created.");
        }

        /// <summary>
        /// Delete the HBase table
        /// </summary>
        /// <param name="hbaseClient"></param>
        static public void DeleteHBaseTable(HBaseClient hbaseClient)
        {
            hbaseClient.DeleteTable(Properties.Settings.Default.HBaseTableName);
            Console.WriteLine("Table deleted.");
        }

        /// <summary>
        /// Retrieve sessions within the given time range
        /// </summary>
        /// <param name="hbaseClient">The hbase client</param>
        /// <param name="eventType">The type of event to look for</param>
        /// <param name="start">Lower bound of the time range</param>
        /// <param name="end">Upper bound of the time range</param>
        static void GetSessionsByTime(HBaseClient hbaseClient, string eventType, DateTime start, DateTime end)
        {
            //Create filters list
            FilterList filters = new FilterList(FilterList.Operator.MustPassAll);

            //Filter to search for the event type value
            SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
                Encoding.UTF8.GetBytes("cf"),
                Encoding.UTF8.GetBytes("event"),
                CompareFilter.CompareOp.Equal,
                Encoding.UTF8.GetBytes(eventType));
            filters.AddFilter(valueFilter);
            
            //Create scanner, set maxVersions so we can get previous versions of rows
            //Since START events may not be the currently returned value
            var scannerSettings = new Scanner()
            {
                filter = filters.ToEncodedString(),
                maxVersions = 5,
                startTime = ToUnixTime(start),
                endTime = ToUnixTime(end)
            };

            var scanner = hbaseClient.CreateScanner(Properties.Settings.Default.HBaseTableName, scannerSettings);
            //Read data from scanner
            CellSet readSet = null;
            //While reading cell sets
            while ((readSet = hbaseClient.ScannerGetNext(scanner)) != null)
            {
                //Iterate over the rows returned
                foreach (var row in readSet.rows)
                {
                    //Get the time stored for the START event
                    var endTime = row.values.Where(v => Encoding.UTF8.GetString(v.column) == "cf:time")
                        .Select(v => BitConverter.ToInt64(v.data, 0)).ToArray()[0];

                    //Get the hbase timestamp of the row
                    var timestamp = row.values.Select(v => v.timestamp).ToArray()[0];
                    //If it's an end event type
                    if (eventType == "END")
                    {
                        //Get the duration stored between END and START events
                        var duration = new TimeSpan(
                            row.values.Where(v => Encoding.UTF8.GetString(v.column) == "cf:duration")
                                .Select(v => BitConverter.ToInt64(v.data, 0)).ToArray()[0]);
                        //Write out the session info, including duration
                        Console.WriteLine("Session {0} lasted {1} minutes, and ended at {2}",
                            Encoding.UTF8.GetString(row.key),
                            duration.Minutes,
                            FromUnixTime(endTime));
                    }
                    else
                    {
                        //If start event type, just write out when it started and the hbase timestamp for the row
                        Console.WriteLine("Session {0} started at {1}. Timestamp = {2}",
                            Encoding.UTF8.GetString(row.key),
                            FromUnixTime(endTime),
                            timestamp);
                    }
                }
            }
        }

        /// <summary>
        /// Get DateTime from Unix time
        /// </summary>
        /// <param name="unixTime">Unix time</param>
        /// <returns>DateTime</returns>
        static public DateTime FromUnixTime(this long unixTime)
        {
            return epoch.AddMilliseconds(unixTime);
        }

        /// <summary>
        /// Get Unix time from DateTime
        /// </summary>
        /// <param name="date">DateTime</param>
        /// <returns>Long containing Unix time</returns>
        public static long ToUnixTime(this DateTime date)
        {
            return Convert.ToInt64((date.ToUniversalTime() - epoch).TotalMilliseconds);
        }
    }
}
