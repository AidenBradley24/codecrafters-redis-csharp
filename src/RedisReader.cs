using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    /// <summary>
    /// Note: base stream must support seeking (no network stream allowed!)
    /// </summary>
    internal class RedisReader(Stream baseStream) : IDisposable
    {
        private readonly BinaryReader br = new(baseStream);

        public string ReadSimpleString()
        {
            Console.WriteLine("SIMPLE");
            if (br.ReadChar() != '+') throw new Exception("Invalid simple string");
            return ReadUntilCRLF();
        }

        public string ReadBulkString()
        {
            Console.WriteLine("BULK");
            if (br.ReadChar() != '$') throw new Exception("Invalid bulk string");
            int length = int.Parse(ReadUntilCRLF());
            string s = Encoding.UTF8.GetString(br.ReadBytes(length));
            br.BaseStream.Position += 2; // skip \r\n
            return s;
        }

        public object[] ReadArray()
        {
            Console.WriteLine("ARRAY");
            if (br.ReadChar() != '*') throw new Exception("Invalid array");
            int length = int.Parse(ReadUntilCRLF());
            object[] array = new object[length];
            for (int i = 0; i < length; i++)
            {
                array[i] = ReadAny();
            }
            return array;
        }

        public object ReadAny()
        {
            Console.WriteLine("ANY");
            char first = (char)br.PeekChar();
            return first switch
            {
                '+' => ReadSimpleString(),
                '$' => ReadBulkString(),
                '*' => ReadArray(),
                _ => throw new Exception($"Invalid type: {first}"),
            };
        }

        private string ReadUntilCRLF()
        {
            List<char> chars = [];
            while (br.PeekChar() != '\r')
            {
                chars.Add(br.ReadChar());
            }
            br.BaseStream.Position += 2; // skip \r\n
            return string.Concat(chars);
        }

        public void Dispose()
        {
            br.Dispose();
        }
    }
}
