using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    internal class RedisWriter(Stream baseStream) : IDisposable
    {
        private readonly BinaryWriter bw = new(baseStream, Encoding.UTF8);

        public void WriteSimpleString(string value)
        {
            if (value.Contains('\r') || value.Contains('\n'))
            {
                throw new ArgumentException("Value cannot contain \\r or \\n");
            }
            bw.Write('+');
            bw.Write(value);
            bw.Write("\r\n");
            bw.Flush();
        }

        public void WriteBulkString(string value)
        {
            bw.Write('$');
            bw.Write(value.Length.ToString());
            bw.Write("\r\n");
            bw.Write(value);
            bw.Write("\r\n");
            bw.Flush();
        }

        public void Dispose()
        {
            bw.Dispose();
        }
    }
}
