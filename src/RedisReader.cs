using System.Text;

namespace RedisComponents
{
    /// <summary>
    /// Note: base stream must support seeking (no network stream allowed!)
    /// </summary>
    internal class RedisReader(Stream baseStream) : IDisposable
    {
        public Stream BaseStream { get => br.BaseStream; }
        private readonly BinaryReader br = new(baseStream, Encoding.UTF8);

        private long byteOffset = 0;
        public void StartByteCount()
        {
            byteOffset = BaseStream.Position;
        }

        public long GetByteCount()
        {
            return BaseStream.Position - byteOffset;
        }

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
            BaseStream.Position += 2; // skip \r\n
            return s;
        }

        public int ReadInt()
        {
            if (br.ReadChar() != ':') throw new Exception("Invalid int");
            char first = (char)br.PeekChar();
            if (first == '-' || first == '+')
            {
                BaseStream.Position++;
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

        public void SkipRDB()
        {
            Console.WriteLine("SKIPPING RDB");
            try
            {
                BaseStream.Position++; // $
                int length = int.Parse(ReadUntilCRLF());
                BaseStream.Position += length;
                Console.WriteLine("SKIP COMPLETE");
            }
            catch (EndOfStreamException)
            {
                Console.WriteLine("SKIP RAN INTO ERROR");
            }
        }

        public object ReadAny()
        {
            char first = (char)br.PeekChar();
            return first switch
            {
                '+' => ReadSimpleString(),
                '$' => ReadBulkString(),
                '*' => ReadArray(),
                ':' => ReadInt(),
                _ => throw new Exception($"Invalid type: {first}"),
            };
        }

        public bool HasNext()
        {
            return br.PeekChar() != 0;
        }

        private string ReadUntilCRLF()
        {
            List<char> chars = [];
            while (br.PeekChar() != '\r')
            {
                chars.Add(br.ReadChar());
            }
            BaseStream.Position += 2; // skip \r\n
            return string.Concat(chars);
        }

        public void Dispose()
        {
            br.Dispose();
            baseStream.Dispose();
        }
    }
}
