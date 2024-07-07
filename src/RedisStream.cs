using System.Globalization;

namespace RedisComponents
{
    internal class RedisStream
    {
        private readonly Dictionary<string, Dictionary<string, object>> entries = [];
        private long previousTime = 0;
        private readonly Dictionary<long, int> previousSequenceNumbers = [];
    
        private void ValidateKey(ref string key)
        {
            long msTime;
            int sequenceNumber;
            int previousSeq;
            bool leadingZeros = false;

            if (key == "*")
            {
                leadingZeros = true;
                msTime = DateTimeOffset.Now.ToUnixTimeSeconds();
                if (previousSequenceNumbers.TryGetValue(msTime, out previousSeq))
                {
                    sequenceNumber = previousSeq + 1;
                }
                else
                {
                    sequenceNumber = msTime == 0 ? 1 : 0;
                }
            }
            else
            {
                int dashIndex = key.IndexOf('-');
                string timeString = key[..dashIndex];
                string sequenceString = key[(dashIndex + 1)..];
                msTime = long.Parse(timeString, CultureInfo.InvariantCulture);

                if (sequenceString == "*")
                {
                    if (previousSequenceNumbers.TryGetValue(msTime, out previousSeq))
                    {
                        sequenceNumber = previousSeq + 1;
                    }
                    else
                    {
                        sequenceNumber = msTime == 0 ? 1 : 0;
                    }
                }
                else
                {
                    previousSequenceNumbers.TryGetValue(msTime, out previousSeq);
                    sequenceNumber = int.Parse(sequenceString, CultureInfo.InvariantCulture);
                }
            }
            
            if (msTime == 0 && sequenceNumber == 0)
            {
                throw new Exception("ERR The ID specified in XADD must be greater than 0-0");
            }
            else if (msTime < previousTime)
            {
                throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
            else if (msTime == previousTime && sequenceNumber <= previousSeq)
            {
                throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }

            previousTime = msTime;
            previousSequenceNumbers[msTime] = sequenceNumber;
            if (leadingZeros)
            {
                key = $"{msTime:0000000000000}-{sequenceNumber}";
            }
            else
            {
                key = $"{msTime}-{sequenceNumber}";
            }
        }

        public string XADD(string key, Dictionary<string, object> entry)
        {
            ValidateKey(ref key);
            entries.Add(key, entry);
            return key;
        }
    }
}
