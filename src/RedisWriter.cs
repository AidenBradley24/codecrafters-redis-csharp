using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace codecrafters_redis.src
{
    internal class RedisWriter(Stream baseStream)
    {
        private readonly Stream baseStream = baseStream;
        
        private void Write(object value)
        {
            baseStream.Write(Encoding.UTF8.GetBytes(value.ToString() ?? ""));
        }

        public void WriteSimpleString(string value)
        {
            if (value.Contains('\r') || value.Contains('\n'))
            {
                throw new ArgumentException("Value cannot contain \\r or \\n");
            }
            Write('+');
            Write(value);
            Write("\r\n");
        }

        public void WriteBulkString(string? value)
        {
            Write('$');
            if(value == null)
            {
                Write("-1");
            }
            else
            {
                Write(value.Length.ToString());
                Write("\r\n");
                Write(value);
            }
            Write("\r\n");
        }

        public void WriteStringArray(string[] strings)
        {
            Write("*");
            Write(strings.Length.ToString());
            Write("\r\n");
            foreach (string s in strings)
            {
                WriteBulkString(s);
            }
        }

        public void WriteEmptyRDB()
        {
            byte[] contents = Convert.FromBase64String("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==");
            Write($"${contents.Length}\r\n");
            baseStream.Write(contents);
        }
    }
}
