using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    internal static class RedisWriter
    {
        public static void WriteSimpleString(Stream stream, string value)
        {
            if (value.Contains('\r') || value.Contains('\n'))
            {
                throw new ArgumentException("Value cannot contain \\r or \\n");
            }
            using var bw = new BinaryWriter(stream, Encoding.UTF8);
            bw.Write('+');
            bw.Write(value);
            bw.Write("\r\n");
            bw.Flush();
        }

        public static void WriteBulkString(Stream stream, string value)
        {
            using var bw = new BinaryWriter(stream, Encoding.UTF8);
            bw.Write('$');
            bw.Write(value.Length.ToString());
            bw.Write("\r\n");
            bw.Write(value);
            bw.Write("\r\n");
            bw.Flush();
        }
    }
}
