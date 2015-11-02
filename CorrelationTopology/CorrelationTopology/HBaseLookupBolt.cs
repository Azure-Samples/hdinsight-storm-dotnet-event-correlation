using Microsoft.HBase.Client;
using Microsoft.SCP;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using Microsoft.HBase.Client.Filters;

namespace CorrelationTopology
{
    /// <summary>
    /// OVERVIEW:
    /// This is a HBase bolt that uses the HBase .Net SDK to look up rows from HBase.
    /// The values of the rows are then emitted to upstream bolt tasks. 
    /// A user can choose to emit rowkey by modifying code in execute block.
    /// 
    /// PRE-REQUISITES:
    /// 1. Microsoft HDInsight HBase Cluster and credentials
    /// 2. HBase Table Schema
    /// 
    /// NUGET:
    /// 1. Microsoft.SCP.Net.SDK - http://www.nuget.org/packages/Microsoft.SCP.Net.SDK/
    /// 2. Microsoft.HBase.Client http://www.nuget.org/packages/Microsoft.HBase.Client/
    /// 
    /// ASSUMPTIONS:
    /// 1. The first field of the Tuple is the ROWKEY of HBASE and rest of the fields are equal to number of HBASE table columns defined in AppSettings
    /// 2. Your previous spout or bolt task takes care of the ROWKEY design. You can choose to add that implementation here too.
    /// 3. All the incoming values that are primitive types are automtically converted into byte[]. If you wish to use complex types, you need to modify the code to handle that case.
    ///   a. DateTime is converted to Millis since UNIX epoch
    /// 
    /// REFERENCES:
    /// 1. https://github.com/hdinsight/hbase-sdk-for-net
    /// 2. https://github.com/apache/storm/tree/master/external/storm-hbase
    /// </summary>
    class HBaseLookupBolt : ISCPBolt
    {
        Context context;
        bool enableAck = false;

        string HBaseClusterUrl { get; set; }
        string HBaseClusterUserName { get; set; }
        string HBaseClusterPassword { get; set; }

        ClusterCredentials HBaseClusterCredentials;
        HBaseClient HBaseClusterClient;

        string HBaseTableName { get; set; }

        /// <summary>
        /// HBaseLookupBolt
        /// </summary>
        /// <param name="context"></param>
        public HBaseLookupBolt(Context context)
        {
            Context.Logger.Info(this.GetType().Name + " constructor called");
            //Set the context
            this.context = context;


            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(string), typeof(string), typeof(long) });
            var outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add(Constants.DEFAULT_STREAM_ID, new List<Type>() { typeof(string), typeof(string), typeof(long), typeof(long) });

            //Declare both input and output schemas
            this.context.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));

            //If this task excepts acks we need to set enableAck as true in TopologyBuilder for it
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }

            //Initialize the HBase connection
            InitializeHBase();
        }

        /// <summary>
        /// A delegate method to return the instance of this class
        /// </summary>
        /// <param name="context">SCP Context, automatically passed by SCP.Net</param>
        /// <param name="parms"></param>
        /// <returns>An instance of the current class</returns>
        public static HBaseLookupBolt Get(Context context, Dictionary<string, Object> parms)
        {
            return new HBaseLookupBolt(context);
        }

        /// <summary>
        /// Initialize the HBase settings and connections
        /// </summary>
        public void InitializeHBase()
        {
            this.HBaseClusterUrl = Properties.Settings.Default.HBaseClusterUrl;//ConfigurationManager.AppSettings["HBaseClusterUrl"];
            if (String.IsNullOrWhiteSpace(this.HBaseClusterUrl))
            {
                throw new ArgumentException("A required setting cannot be null or empty", "HBaseClusterUrl");
            }

            this.HBaseClusterUserName = Properties.Settings.Default.HBaseClusterUserName;//ConfigurationManager.AppSettings["HBaseClusterUserName"];
            if (String.IsNullOrWhiteSpace(this.HBaseClusterUserName))
            {
                throw new ArgumentException("A required setting cannot be null or empty", "HBaseClusterUserName");
            }

            this.HBaseClusterPassword = Properties.Settings.Default.HBaseClusterPassword;//ConfigurationManager.AppSettings["HBaseClusterPassword"];
            if (String.IsNullOrWhiteSpace(this.HBaseClusterPassword))
            {
                throw new ArgumentException("A required setting cannot be null or empty", "HBaseClusterPassword");
            }

            this.HBaseTableName = Properties.Settings.Default.HBaseTableName;//ConfigurationManager.AppSettings["HBaseTableName"];
            if (String.IsNullOrWhiteSpace(this.HBaseTableName))
            {
                throw new ArgumentException("A required setting cannot be null or empty", "HBaseTableName");
            }

            //Setup the credentials for HBase cluster
            this.HBaseClusterCredentials =
                new ClusterCredentials(
                    new Uri(this.HBaseClusterUrl),
                    this.HBaseClusterUserName,
                    this.HBaseClusterPassword);

            this.HBaseClusterClient = new HBaseClient(this.HBaseClusterCredentials);

            //Query HBase for existing tables
            var tables = this.HBaseClusterClient.ListTables();
            Context.Logger.Info("HBase Tables (" + tables.name.Count + "): " + String.Join(", ", tables.name));


            //Throw an exception if the table doesn't exist
            if (!tables.name.Contains(this.HBaseTableName))
            {
                throw new Exception("Cannot find HBase table: " + this.HBaseTableName);
            }
        }

        /// <summary>
        /// Executes incoming tuples
        /// </summary>
        /// <param name="tuple">The first field is treated as rowkey and rest as column names</param>
        public void Execute(SCPTuple tuple)
        {
            //get the tuple info
            string sessionId = tuple.GetString(0);
            string sessionEvent = tuple.GetString(1);
            long sessionEventTime = tuple.GetLong(2);


            //If it's a start event, assume there's nothing to find so just re-emit
            //NOTE: If messages may arrive out of order, you would need to add logic to
            //query HBase to see if the end event has previously arrived,
            //calculate the duration, etc.
            if (sessionEvent == "START")
            {
                //Just re-emit the incoming data, plus 0 for duration, since we declare we send a 0 duration
                //since we don't know the END event yet.
                Values emitValues = new Values(tuple.GetValue(0), tuple.GetValue(1), tuple.GetValue(2), 0L);
                
                //Is ack enabled?
                if (enableAck)
                {
                    //Emit the values, anchored to the incoming tuple
                    this.context.Emit(Constants.DEFAULT_STREAM_ID, new List<SCPTuple>() { tuple }, emitValues);
                    //Ack the incoming tuple
                    this.context.Ack(tuple);
                }
                else
                {
                    //No ack enabled? Fire and forget.
                    this.context.Emit(Constants.DEFAULT_STREAM_ID, emitValues);
                }
            }
            if (sessionEvent == "END")
            {
                //Use filters
                FilterList filters = new FilterList(FilterList.Operator.MustPassAll);
                //Filter on the row by sessionID
                RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.Equal, new BinaryComparator(TypeHelper.ToBytes(sessionId)));
                filters.AddFilter(rowFilter);
                //Filter on the event column for the START event
                SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
                    Encoding.UTF8.GetBytes("cf"),
                    Encoding.UTF8.GetBytes("event"),
                    CompareFilter.CompareOp.Equal,
                    Encoding.UTF8.GetBytes("START"));
                filters.AddFilter(valueFilter);
                //Create scanner settings using the filters
                var scannerSettings = new Scanner()
                {
                    filter = filters.ToEncodedString()
                };
                //Get the scanner
                var scanner = HBaseClusterClient.CreateScanner(HBaseTableName, scannerSettings);

                CellSet readSet = null;
                while ((readSet = HBaseClusterClient.ScannerGetNext(scanner)) != null)
                {
                    //In theory we should only find one row
                    foreach (var row in readSet.rows)
                    {
                        //Pull back just the event column
                        var rowState = row.values.Where(v => Encoding.UTF8.GetString(v.column) == "cf:event")
                            .Select(v => Encoding.UTF8.GetString(v.data)).ToArray()[0];
                        //Is it a START event as expected?
                        if (rowState == "START")
                        {
                            //Get the start time
                            var startTime = TypeHelper.FromUnixTime(
                                row.values.Where(v => Encoding.UTF8.GetString(v.column) == "cf:time")
                                    .Select(v => BitConverter.ToInt64(v.data,0)).ToArray()[0]);
                            //Get the difference between start and end
                            DateTime endTime = TypeHelper.FromUnixTime(sessionEventTime);
                            TimeSpan duration = endTime.Subtract(startTime);
                            //Emit the tuple, with the duration between start/end.
                            Values emitValues = new Values(sessionId, sessionEvent, sessionEventTime, duration.Ticks);
                            //If ack is enabled
                            if (enableAck)
                            {
                                //Emit the values, anchored to the incoming tuple
                                this.context.Emit(Constants.DEFAULT_STREAM_ID, new List<SCPTuple>() { tuple }, emitValues);
                                //Ack the incoming tuple
                                this.context.Ack(tuple);
                            }
                            else
                            {
                                //No ack enabled? Fire and forget.
                                this.context.Emit(Constants.DEFAULT_STREAM_ID, emitValues);
                            }
                        }
                        else
                        {
                            //Since this is a simple example, do nothing.
                            //In a real solution, you'd have to figure out what to do
                            //when receiving an END before a START.
                        }
                    }
                }
            }
        }
    }
}
