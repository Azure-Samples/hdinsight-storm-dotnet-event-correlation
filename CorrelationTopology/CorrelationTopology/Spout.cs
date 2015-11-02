using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading;
using Microsoft.SCP;
using Microsoft.SCP.Rpc.Generated;

namespace CorrelationTopology
{
    public class Spout : ISCPSpout
    {
        //Context for this component
        private Context context;
        //Random numbers
        private Random r = new Random();
        //Emulate 100 user sessions
        private Session[] sessions = new Session[100];

        //Is this a transactional topology?
        //i.e. do we need to replay failed tuples?
        private bool enableAck = false;
        //If so, we need to keep track using sequence IDs
        private long seqId = 0;
        //And a place to cache the original so we can replay later
        private Dictionary<long, object> cachedTuples = new Dictionary<long, object>();

        /// <summary>
        /// Return a new instance of the spout
        /// </summary>
        /// <param name="context">Context for this topology</param>
        public Spout(Context context)
        {
            this.context = context;
            //Declare the ouput, which is two strings and a long for this example
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add("default", new List<Type>() { typeof(string), typeof(string), typeof(long) });
            this.context.DeclareComponentSchema(new ComponentStreamSchema(null, outputSchema));

            //Create initial sessions
            for (int i = 0; i < sessions.Length; i++)
            {
                this.sessions[i] = new Session();
            }

            //Are ack's enabled in the context?
            if (Context.Config.pluginConf.ContainsKey(Constants.NONTRANSACTIONAL_ENABLE_ACK))
            {
                //If so, we need to be transactional
                enableAck = (bool)(Context.Config.pluginConf[Constants.NONTRANSACTIONAL_ENABLE_ACK]);
            }
        }

        /// <summary>
        /// Get an instance of this component
        /// </summary>
        /// <param name="context">Context for the topology</param>
        /// <param name="parms"></param>
        /// <returns></returns>
        public static Spout Get(Context context, Dictionary<string, Object> parms)
        {
            return new Spout(context);
        }

        /// <summary>
        /// Emit a set of tuples
        /// </summary>
        /// <param name="parms"></param>
        public void NextTuple(Dictionary<string, Object> parms)
        {
            //get a random session
            int sessionIdx = r.Next(sessions.Length);
            Session session = this.sessions[sessionIdx];
            //Check timeout between emits for each session state
            //to simulate real-world user session behavior.
            DateTime now = DateTime.Now;
            //If the timeout has happened, emit
            if (session.ended)
            {
                //New values containing the session ID, state, and datetime (as UNIX time)
                Values emitValues = new Values(session.id.ToString(), session.state, TypeHelper.ToUnixTime(now));
                //If acks are enabled
                if (enableAck)
                {
                    //Add to the spout cache so that the tuple can be re-emitted on fail
                    cachedTuples.Add(seqId, emitValues);
                    //Emit the tuples with the current sequence ID
                    this.context.Emit(Constants.DEFAULT_STREAM_ID, emitValues, seqId);
                    //Increment the sequence
                    seqId++;
                }
                else
                {
                    //If no acks, just fire and forget
                    this.context.Emit(Constants.DEFAULT_STREAM_ID, emitValues);
                }

                //If this was an end event, recycle the session entry with a new session
                if (session.state == "END")
                {
                    this.sessions[sessionIdx] = new Session();
                }
                else
                {
                    //If not END, then it was START and we need to set to END
                    this.sessions[sessionIdx].state = "END";
                    //Set the lastEmitted time so we can wait for timeout
                    this.sessions[sessionIdx].emitted = now;
                }
            }
        }

        /// <summary>
        /// Handle ack's for the specified tuple
        /// </summary>
        /// <param name="seqId">the sequence ID</param>
        /// <param name="parms"></param>
        public void Ack(long seqId, Dictionary<string, Object> parms)
        {
            if (enableAck)
            {
                //Remove the successfully acked tuple from the cache.
                cachedTuples.Remove(seqId);
            }
        }

        /// <summary>
        /// Handle failed tuples
        /// </summary>
        /// <param name="seqId">the sequence ID</param>
        /// <param name="parms"></param>
        public void Fail(long seqId, Dictionary<string, Object> parms)
        {
            if (enableAck)
            {
                //Re-emit the failed tuple again - only if it exists
                if (cachedTuples.ContainsKey(seqId))
                {
                    this.context.Emit(Constants.DEFAULT_STREAM_ID, new Values(cachedTuples[seqId]), seqId);
                }
            }
        }
    }
}