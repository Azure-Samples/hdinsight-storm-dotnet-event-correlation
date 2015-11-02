using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CorrelationTopology
{
    /// <summary>
    /// Simulates user sessions of varying length
    /// </summary>
    class Session
    {
        private Random r = new Random();
        public Guid id;
        public string state;
        public DateTime emitted;
        private int timeout;

        /// <summary>
        /// Return a new session
        /// </summary>
        public Session()
        {
            //Generate a new session ID
            this.id = Guid.NewGuid();
            //Default state should be START for new sessions
            this.state = "START";
            //Get the current time
            DateTime now = DateTime.Now;
            //put the lastemitted in the past
            this.emitted = now.Subtract(TimeSpan.FromDays(1));
            //How long is this session going to last, in minutes
            this.timeout = r.Next(5) + 1;
        }

        /// <summary>
        /// True, if the current time has exceeded the timeout (end time)
        /// for this session; otherwise, False.
        /// </summary>
        public bool ended
        {
            //Has the timeout elapsed?
            get
            {
                return (DateTime.Now - this.emitted).TotalMinutes > this.timeout;
            }
        }
    }
}
