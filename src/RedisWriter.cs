using System.Text;
using System.Globalization;

namespace RedisComponents
{
    internal class RedisWriter(Stream baseStream)
    {
        public Stream BaseStream { get; } = baseStream;
        public bool Enabled { get; set; } = true;

        private long byteOffset = 0;
        public void StartByteCount()
        {
            byteOffset = BaseStream.Position;
        }

        public long GetByteCount()
        {
            return BaseStream.Position - byteOffset;
        }

        private void Write(object value)
        {
            if (!Enabled) return;
            BaseStream.Write(Encoding.UTF8.GetBytes(Convert.ToString(value, CultureInfo.InvariantCulture) ?? ""));
        }

        public void Flush()
        {
            BaseStream.Flush();
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
                Write(value.Length);
                Write("\r\n");
                Write(value);
            }
            Write("\r\n");
        }

        public void WriteStringArray(string[] strings)
        {
            Write("*");
            Write(strings.Length);
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
            BaseStream.Write(contents);
        }

        public void WriteInt(int value)
        {
            Write(":");
            Write(value < 0 ? "-" : "+");
            Write(value);
            Write("\r\n");
        }

        public void WriteArray(object?[] array)
        {
            Write("*");
            Write(array.Length);
            Write("\r\n");
            foreach (object? o in array)
            {
                WriteAny(o);
            }
        }

        public void WriteSimpleError(string message)
        {
            if (message.Contains('\r') || message.Contains('\n'))
            {
                throw new ArgumentException("Value cannot contain \\r or \\n");
            }
            Write('-');
            Write(message);
            Write("\r\n");
        }

        public void WriteBulkError(string message)
        {
            Write("!");
            Write(message.Length);
            Write("\r\n");
            Write(message);
            Write("\r\n");
        }

        public void WriteAny(object? value)
        {
            if(value == null)
            {
                WriteBulkString(null);
            }
            else if (value is object[] array)
            {
                WriteArray(array);
            }
            else if (value is string str)
            {
                WriteBulkString(str);
            }
            else if (value is int i)
            {
                WriteInt(i);
            }
            else
            {
                WriteBulkString(value.ToString());
            }
        }
    }
}
