using System.Text;

namespace RedisComponents
{
    internal class RDBFile(FileInfo file)
    {
        private const int VERSION = 7;
        private Dictionary<string, (object val, DateTime? timeout)>? cache = null;

        private BinaryReader StartRead()
        {
            FileStream fs = file.OpenRead();
            fs.Seek(9, SeekOrigin.Begin); // skip header
            return new BinaryReader(fs);
        }

        private static void SeekToByte(BinaryReader br, byte check)
        {
            byte current;
            do
            {
                current = br.ReadByte();
            } while (current != check);
        }

        private static object ReadSizeEncodedValue(BinaryReader br)
        {
            byte first = br.ReadByte();
            if ((first & 0b11000000) == 0)
            {
                // 6-bit integer
                return first;
            }
            else if((first & 0b11000000) == 0b01000000)
            {
                // 14-bit integer
                byte second = br.ReadByte();
                byte firstPart = (byte)(first & 0b00111111);
                byte[] bytes = [firstPart, second];
                Array.Reverse(bytes);
                return BitConverter.ToInt16(bytes);
            }
            else if ((first & 0b11000000) == 0b10000000)
            {
                // 32-bit integer
                byte[] bytes = br.ReadBytes(4);
                Array.Reverse(bytes);
                return BitConverter.ToInt32(bytes);
            }
            else
            {
                first &= 0b00111111;
                return ReadStringEncodedValue(first, br);
            }
        }

        private static object ReadStringEncodedValue(byte first, BinaryReader br)
        {
            if (first == 0xC0)
            {
                // 8-bit integer
                return (int)br.ReadByte();
            }
            else if (first == 0xC1)
            {
                // 16-bit integer
                byte[] bytes = br.ReadBytes(2);
                return BitConverter.ToInt16(bytes);
            }
            else if (first == 0xC2)
            {
                // 32-bit integer
                byte[] bytes = br.ReadBytes(4);
                return BitConverter.ToInt32(bytes);
            }
            else if (first == 0xC3)
            {
                // compressed string
                return "";
            }
            else
            {
                byte[] bytes = br.ReadBytes(first);
                return Encoding.UTF8.GetString(bytes);
            }
        }

        private static object ReadStringEncodedValue(BinaryReader br)
        {
            byte first = br.ReadByte();
            return ReadStringEncodedValue(first, br);
        }

        public Dictionary<string, (object val, DateTime? timeout)> GetDictionary()
        {
            if (cache != null) return cache;
            Console.WriteLine("Reading RDB...");

            BinaryReader br;
            try
            {
                br = StartRead();
            }
            catch (FileNotFoundException)
            {
                // treat db as empty
                Console.WriteLine("DB is empty.");
                cache = [];
                return cache;
            }

            Console.WriteLine("1");
            SeekToByte(br, 0xFE);
            Console.WriteLine("2");

            SeekToByte(br, 0xFB);
            Console.WriteLine("3");

            int keyValueSize = Convert.ToInt32(ReadSizeEncodedValue(br));
            Console.WriteLine("4");

            int expirySize = Convert.ToInt32(ReadSizeEncodedValue(br));
            Console.WriteLine("5");

            Dictionary<string, (object val, DateTime? timeout)> output = [];

            for (int i = 0; i < keyValueSize; i++)
            {
                Console.WriteLine($"read key/val: {i}");

                byte type = br.ReadByte();
                DateTime? timeout = null;
                if (type == 0xFC)
                {
                    timeout = DateTimeOffset.FromUnixTimeMilliseconds((long)BitConverter.ToUInt64(br.ReadBytes(8))).DateTime;
                    type = br.ReadByte();
                }
                else if (type == 0xFD)
                {
                    timeout = DateTimeOffset.FromUnixTimeSeconds(BitConverter.ToUInt32(br.ReadBytes(4))).DateTime;
                    type = br.ReadByte();
                }

                string key = Convert.ToString(ReadStringEncodedValue(br))!;
                object value = type switch
                {
                    0x00 => Convert.ToString(ReadStringEncodedValue(br))!,
                    _ => throw new NotImplementedException(),
                };

                output.Add(key, (value, timeout));
            }

            Console.WriteLine("DB has been read.");
            cache = output;
            return output;
        }
    }
}
