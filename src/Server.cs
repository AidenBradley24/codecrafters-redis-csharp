using codecrafters_redis.src;
using System.Net;
using System.Net.Sockets;
using System.Text;

int port = 6379;
int? myMasterPort = null;
string? myMasterHostName = null;
for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--port")
    {
        port = Convert.ToInt32(args[++i]);
    }
    else if (args[i] == "--replicaof")
    {
        string m = args[++i];
        int last = m.LastIndexOf(' ');
        myMasterHostName = m[..last];
        myMasterPort = Convert.ToInt32(m[(last+1)..]);
    }
}

TcpListener server = new(IPAddress.Any, port);
server.Start();

Dictionary<string, (string val, DateTime? timeout)> myCache = [];
Dictionary<string, object> myInfo = [];
List<TcpClient> myReplicas = [];

myInfo.Add("role", myMasterPort == null ? "master" : "slave");
myInfo.Add("master_replid", RandomAlphanum(40));
myInfo.Add("master_repl_offset", 0);

HashSet<string> propagatedCommands = ["SET", "DEL"];

TcpClient? myMaster = null;
if (myMasterPort != null && myMasterHostName != null)
{
    MasterHandshake();
    return;
}


while (true)
{
    TcpClient client = server.AcceptTcpClient();
    _ = Task.Run(async () => await HandleClient(client));
}

Task HandleClient(TcpClient client)
{
    NetworkStream ns = client.GetStream();
    byte[] buffer = new byte[1024];
    while (client.Connected)
    {
        using RedisReader rr = InitRead(ns, buffer);
        object[] request = (object[])rr.ReadAny();

        bool HasArgument(string arg, int index)
        {
            return request.Length > index && ((string)request[index]).Equals(arg, StringComparison.InvariantCultureIgnoreCase);
        }

        foreach (object obj in request)
        {
            Console.WriteLine(obj);
        }

        string command = ((string)request[0]).ToUpperInvariant();

        if (propagatedCommands.Contains(command))
        {
            Console.WriteLine("AA1");

            foreach (TcpClient replica in myReplicas)
            {
                Console.WriteLine("AA2");

                try
                {
                    using NetworkStream masterConnection = replica.GetStream();
                    Console.WriteLine("AA3");

                    RedisWriter writer = new(masterConnection);

                    Console.WriteLine("AA4");

                    writer.WriteArray(request);

                    Console.WriteLine("AA5");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                }


            }
        }

        RedisWriter rw = new(ns) { Enabled = (string)myInfo["role"] != "slave" };
        switch (command)
        {
            case "PING":
                rw.WriteSimpleString("PONG");
                break;
            case "ECHO":
                rw.WriteBulkString((string)request[1]);
                break;
            case "GET":
                {
                    (string val, DateTime? timeout) dat;
                    bool hasVal = false;
                    lock (myCache)
                    {
                        hasVal = myCache.TryGetValue((string)request[1], out dat);
                    }

                    if (!hasVal)
                    {
                        rw.WriteBulkString(null);
                    }
                    else if(dat.timeout != null && DateTime.Now >= dat.timeout)
                    {
                        myCache.Remove((string)request[1]);
                        rw.WriteBulkString(null);
                    }
                    else
                    {
                        rw.WriteBulkString(dat.val);
                    }
                }
                break;
            case "SET":
                {
                    DateTime? timeout = null;
                    if (HasArgument("px", 3))
                    {
                        int milliseconds = Convert.ToInt32(request[4]);
                        timeout = DateTime.Now.AddMilliseconds(milliseconds);
                    }

                    lock (myCache)
                    {
                        myCache[(string)request[1]] = ((string)request[2], timeout);
                    }

                    rw.WriteSimpleString("OK");
                }
                break;
            case "INFO":
                {
                    StringBuilder sb = new();
                    foreach(KeyValuePair<string, object> pair in myInfo)
                    {
                        sb.Append(pair.Key);
                        sb.Append(':');
                        sb.Append(pair.Value);
                        sb.Append('\n');
                    }
                    rw.WriteBulkString(sb.ToString());
                }
                break;
            case "REPLCONF":
                {
                    if (HasArgument("listening-port", 1))
                    {
                        lock (myReplicas)
                        {
                            myReplicas.Add(client);
                        }
                    }

                    rw.WriteSimpleString("OK");
                }
                break;
            case "PSYNC":
                rw.WriteSimpleString($"FULLRESYNC {myInfo["master_replid"]} {myInfo["master_repl_offset"]}");
                rw.WriteEmptyRDB(); // TODO write actual db
                break;
        }
    }

    return Task.CompletedTask;
}

static RedisReader InitRead(NetworkStream ns, byte[] buffer)
{
    ns.Read(buffer);
    var ms = new MemoryStream(buffer);
    return new RedisReader(ms);
}

static string RandomAlphanum(int length)
{
    const string chars = "abcdefghijklmnopqrstuvwxyz0123456789";
    return new string(Enumerable.Repeat(chars, length)
        .Select(s => s[Random.Shared.Next(s.Length)]).ToArray());
}

void MasterHandshake()
{
    myMaster = new TcpClient(myMasterHostName, (int)myMasterPort);
    using NetworkStream ns = myMaster.GetStream();
    RedisWriter rw = new(ns);
    byte[] buffer = new byte[1024];

    rw.WriteStringArray(["PING"]);
    {
        using RedisReader rr = InitRead(ns, buffer);
        string response = rr.ReadSimpleString();
        if (!response.Equals("PONG", StringComparison.InvariantCultureIgnoreCase))
        {
            throw new Exception("not a pong!");
        }
    }

    rw.WriteStringArray(["REPLCONF", "listening-port", port.ToString()]);
    {
        using RedisReader rr = InitRead(ns, buffer);
        string response = rr.ReadSimpleString();
        if (!response.Equals("OK", StringComparison.InvariantCultureIgnoreCase))
        {
            throw new Exception("not ok!");
        }
    }

    rw.WriteStringArray(["REPLCONF", "capa", "eof", "capa", "psync2"]);
    {
        using RedisReader rr = InitRead(ns, buffer);
        string response = rr.ReadSimpleString();
        if (!response.Equals("OK", StringComparison.InvariantCultureIgnoreCase))
        {
            throw new Exception("not ok!");
        }
    }

    rw.WriteStringArray(["PSYNC", "?", "-1"]);
    {
        using RedisReader rr = InitRead(ns, buffer);
        string response = rr.ReadSimpleString();
        // ignored response
    }

    _ = Task.Run(async () => await HandleClient(myMaster));
}