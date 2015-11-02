using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CorrelationTopology
{
    static class TypeHelper
    {
        //Start time for Unix time
        private static DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Returns a DateTime object for the given UNIX time value
        /// </summary>
        /// <param name="unixTime">UNIX time value</param>
        /// <returns>The DateTime object</returns>
        public static DateTime FromUnixTime(this long unixTime)
        {
            return epoch.AddMilliseconds(unixTime);
        }

        /// <summary>
        /// Returns the UNIX time for the given DateTime object
        /// </summary>
        /// <param name="date">The DateTime object</param>
        /// <returns>The UNIX time value</returns>
        public static long ToUnixTime(this DateTime date)
        {
            return Convert.ToInt64((date.ToUniversalTime() - epoch).TotalMilliseconds);
        }

        /// <summary>
        /// Converts primitive types to byte array
        /// </summary>
        /// <param name="o">input object of primitive type</param>
        /// <returns>byte array representing the input object</returns>
        public static byte[] ToBytes(object o)
        {
            if (o is string)
            {
                return Encoding.UTF8.GetBytes((string)o);
            }
            else if (o is int)
            {
                return BitConverter.GetBytes((int)o);
            }
            else if (o is long)
            {
                return BitConverter.GetBytes((long)o);
            }
            else if (o is short)
            {
                return BitConverter.GetBytes((short)o);
            }
            else if (o is double)
            {
                return BitConverter.GetBytes((double)o);
            }
            else if (o is float)
            {
                return BitConverter.GetBytes((float)o);
            }
            else if (o is bool)
            {
                return BitConverter.GetBytes((bool)o);
            }
            else if (o is DateTime)
            {
                //Return UNIX time for DateTime objects
                return BitConverter.GetBytes(ToUnixTime((DateTime)o));
            }
            else
            {
                throw new NotImplementedException("ToBytes() can only handle primitive types. To use other types, please modify the implementation or change input schema.");
            }
        }
    }
}
