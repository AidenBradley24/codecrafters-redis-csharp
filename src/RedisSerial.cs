using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace codecrafters_redis.src
{
    /// <summary>
    /// Note: all read operation streams must support seeking (no network stream allowed!)
    /// </summary>
    internal static class RedisSerial
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

        public static string ReadSimpleString(Stream stream)
        {
            Console.WriteLine("SIMPLE");
            using var br = new BinaryReader(stream, Encoding.UTF8);
            if (br.ReadChar() != '+') throw new Exception("Invalid simple string");
            return ReadUntilCRLF(br);
        }

        public static string ReadBulkString(Stream stream)
        {
            Console.WriteLine("BULK");
            using var br = new BinaryReader(stream, Encoding.UTF8);
            if (br.ReadChar() != '$') throw new Exception("Invalid bulk string");
            int length = int.Parse(ReadUntilCRLF(br));
            string s = Encoding.UTF8.GetString(br.ReadBytes(length));
            stream.Position += 2; // skip \r\n
            return s;
        }

        public static object[] ReadArray(Stream stream)
        {
            Console.WriteLine("ARRAY");
            using var br = new BinaryReader(stream, Encoding.UTF8);
            if (br.ReadChar() != '*') throw new Exception("Invalid array");
            int length = int.Parse(ReadUntilCRLF(br));
            object[] array = new object[length];
            for (int i = 0; i < length; i++)
            {
                array[i] = ReadAny(stream);
            }
            return array;
        }

        public static object ReadAny(Stream stream)
        {
            Console.WriteLine("ANY");
            using var br = new BinaryReader(stream, Encoding.UTF8);
            char first = (char)br.PeekChar();
            return first switch
            {
                '+' => ReadSimpleString(stream),
                '$' => ReadBulkString(stream),
                '*' => ReadArray(stream),
                _ => throw new Exception($"Invalid type: {first}"),
            };
        }

        private static string ReadUntilCRLF(BinaryReader br)
        {
            List<char> chars = [];
            while (br.PeekChar() != '\r')
            {
                chars.Add(br.ReadChar());
            }
            br.BaseStream.Position += 2; // skip \r\n
            return string.Concat(chars);
        }
    }
}
