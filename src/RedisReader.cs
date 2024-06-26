using System.Text;

namespace codecrafters_redis.src
{
    /// <summary>
    /// Note: base stream must support seeking (no network stream allowed!)
    /// </summary>
    internal class RedisReader(Stream baseStream) : IDisposable
    {
        private readonly BinaryReader br = new(baseStream, Encoding.UTF8);

        public string ReadSimpleString()
        {
            if (br.ReadChar() != '+') throw new Exception("Invalid simple string");
            return ReadUntilCRLF();
        }

        public string ReadBulkString()
        {
            if (br.ReadChar() != '$') throw new Exception("Invalid bulk string");
            int length = int.Parse(ReadUntilCRLF());
            string s = Encoding.UTF8.GetString(br.ReadBytes(length));
            br.BaseStream.Position += 2; // skip \r\n
            return s;
        }

        public int ReadInt()
        {
            if (br.ReadChar() != ':') throw new Exception("Invalid int");
            char first = (char)br.PeekChar();
            if (first == '-' || first == '+')
            {
                br.BaseStream.Position++;
            }
            return int.Parse(ReadUntilCRLF()) * first == '-' ? -1 : 1;
        }

        public object[] ReadArray()
        {
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
