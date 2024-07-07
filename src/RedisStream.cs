using System.Globalization;

namespace RedisComponents
{
    internal class RedisStream
    {
        private readonly Dictionary<string, Dictionary<string, object>> entries = [];
        private ulong previousTime = 0;
        private uint previousSequenceNumber = 0;
    
        private void ValidateKey(ref string key)
        {
            int dashIndex = key.IndexOf('-');
            string timeString = key[..dashIndex];
            string sequenceString = key[(dashIndex + 1)..];
            ulong msTime = ulong.Parse(timeString, CultureInfo.InvariantCulture);

            uint sequenceNumber;        
            if (sequenceString == "*")
            {
                if (entries.Count == 0)
                {
                    sequenceNumber = msTime == 0ul ? 1u : 0u;
                }
                else
                {
                    sequenceNumber = previousSequenceNumber + 1;
                }
            }
            else
            {
                sequenceNumber = uint.Parse(sequenceString, CultureInfo.InvariantCulture);
            }
            
            if (msTime == 0ul && sequenceNumber == 0u)
            {
                throw new Exception("ERR The ID specified in XADD must be greater than 0-0");
            }
            else if (msTime < previousTime)
            {
                throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
            else if (msTime == previousTime && sequenceNumber <= previousSequenceNumber)
            {
                throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }

            previousTime = msTime;
            previousSequenceNumber = sequenceNumber;
        }

        public string XADD(string key, Dictionary<string, object> entry)
        {
            ValidateKey(ref key);
            entries.Add(key, entry);
            return key;
        }
    }
}
