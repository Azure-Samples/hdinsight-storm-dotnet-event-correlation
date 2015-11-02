using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.SCP.Topology;

namespace CorrelationTopology
{
    [Active(true)]
    class Program : TopologyDescriptor
    {
        static void Main(string[] args)
        {
        }
        /// <summary>
        /// Defines and configures the topology
        /// </summary>
        /// <returns></returns>
        public ITopologyBuilder GetTopologyBuilder()
        {
            //Define a new topology named "EventTopology"
            TopologyBuilder topologyBuilder = new TopologyBuilder("EventTopology");
            //The spout, which emits events
            topologyBuilder.SetSpout(
                "Spout",
                Spout.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"id", "event", "eventtime"}}
                },
                1);
            //A bolt that is used to look up previous events for the same session from HBase
            //If this is an end event, the time span between the start and end events is
            //emitted as 'duration'
            topologyBuilder.SetBolt(
                "Lookup",
                HBaseLookupBolt.Get,
                new Dictionary<string, List<string>>()
                {
                    {Constants.DEFAULT_STREAM_ID, new List<string>(){"id", "event", "eventtime", "duration"}}
                },
                1).shuffleGrouping("Spout");
            //A bolt that writes events to HBase
            topologyBuilder.SetBolt(
                "Store",
                HBaseBolt.Get,
                new Dictionary<string, List<string>>(),
                1).shuffleGrouping("Lookup");

            return topologyBuilder;
        }
    }
}

