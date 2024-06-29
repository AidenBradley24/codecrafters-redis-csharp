using codecrafters_redis.src;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Collections.Concurrent;
using System;

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

Dictionary<string, (string val, DateTime? timeout)> myCache = [];
Dictionary<string, object> myInfo = []; 
myInfo.Add("role", myMasterPort == null ? "master" : "slave");
myInfo.Add("master_replid", RandomAlphanum(40));
myInfo.Add("master_repl_offset", 0);

ConcurrentBag<TcpClient> myReplicas = [];

HashSet<string> propagatedCommands = ["SET", "DEL", "INFO"];

TcpListener server = new(IPAddress.Any, port);
server.Start();

Console.WriteLine($"Started server: {port}");

TcpClient? myMaster = null;
if (myMasterPort != null && myMasterHostName != null)
{
    StartReplica();
}

while (true)
{
    TcpClient client = server.AcceptTcpClient();
    _ = Task.Run(async () => await HandleClient(client, false));
}

Task HandleClient(TcpClient client, bool clientIsMaster)
{
    Console.WriteLine($"Client handle started: {client.Client.RemoteEndPoint}");
    NetworkStream ns = client.GetStream();

    byte[] buffer = new byte[1024];
    bool clientLaunched = false;
    while (client.Connected)
    {
        Console.WriteLine("Client is still connected");
        RedisReader? rr = null;

        try
        {
            if (!clientLaunched && clientIsMaster)
            {
                FinalizeHandshake(ref rr, ns, buffer);
                clientLaunched = true;
            }

            rr ??= ReadNetwork(ns, buffer);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }

        while (rr.HasNext())
        {
            object[] request;

            try
            {
                request = rr.ReadArray();
            }
            catch
            {
                break;
            }

            bool HasArgument(string arg, int index)
            {
                return request.Length > index && ((string)request[index]).Equals(arg, StringComparison.InvariantCultureIgnoreCase);
            }

            Console.WriteLine($"recieved request from {(clientIsMaster ? "Master" : client.Client.RemoteEndPoint)}:");
            foreach (object obj in request)
            {
                Console.WriteLine(obj);
            }
            Console.WriteLine("(end of request)");

            string command = ((string)request[0]).ToUpperInvariant();
            if (propagatedCommands.Contains(command))
            {
                foreach (TcpClient repClient in myReplicas)
                {
                    try
                    {
                        NetworkStream masterConnection = repClient.GetStream();
                        RedisWriter writer = new(masterConnection);
                        writer.WriteArray(request);
                        Console.WriteLine($"command replicated to {repClient}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            }

            RedisWriter rw = new(ns) { Enabled = !clientIsMaster || command == "REPLCONF" };
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
                        else if (dat.timeout != null && DateTime.Now >= dat.timeout)
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
                        foreach (KeyValuePair<string, object> pair in myInfo)
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
                            int port = Convert.ToInt32(request[2]);
                            Console.WriteLine($"adding client: {port}");
                        }
                        
                        if (clientIsMaster && HasArgument("GETACK", 1))
                        {
                            rw.WriteArray(["REPLCONF", "ACK", "0"]);
                        }

                        if (!clientIsMaster) rw.WriteSimpleString("OK");
                    }
                    break;
                case "PSYNC":
                    rw.WriteSimpleString($"FULLRESYNC {myInfo["master_replid"]} {myInfo["master_repl_offset"]}");
                    rw.WriteEmptyRDB(); // TODO write actual db
                    myReplicas.Add(client);
                    break;
            }
        }
    }

    Console.WriteLine($"closing connection: {client.Client.RemoteEndPoint}");
    client.Dispose();
    return Task.CompletedTask;
}

static RedisReader ReadNetwork(NetworkStream ns, byte[] buffer)
{
    Console.WriteLine("reading...");
    ns.Read(buffer);
    Console.WriteLine("read complete");
    var ms = new MemoryStream(buffer);
    return new RedisReader(ms);
}

static string RandomAlphanum(int length)
{
    const string chars = "abcdefghijklmnopqrstuvwxyz0123456789";
    return new string(Enumerable.Repeat(chars, length)
        .Select(s => s[Random.Shared.Next(s.Length)]).ToArray());
}

void StartReplica()
{
    Console.WriteLine($"Started handshake with {myMasterPort}");
    myMaster = new TcpClient(myMasterHostName, (int)myMasterPort);
    NetworkStream ns = myMaster.GetStream();
    RedisWriter rw = new(ns);
    byte[] buffer = new byte[1024];

    rw.WriteStringArray(["PING"]);
    {
        RedisReader rr = ReadNetwork(ns, buffer);
        string response = rr.ReadSimpleString();
        if (!response.Equals("PONG", StringComparison.InvariantCultureIgnoreCase))
        {
            throw new Exception("not a pong!");
        }
    }
    Console.WriteLine("handshake 1/4");

    rw.WriteStringArray(["REPLCONF", "listening-port", port.ToString()]);
    {
        RedisReader rr = ReadNetwork(ns, buffer);
        string response = rr.ReadSimpleString();
        if (!response.Equals("OK", StringComparison.InvariantCultureIgnoreCase))
        {
            throw new Exception("not ok!");
        }
    }
    Console.WriteLine("handshake 2/4");

    rw.WriteStringArray(["REPLCONF", "capa", "eof", "capa", "psync2"]);
    {
        RedisReader rr = ReadNetwork(ns, buffer);
        string response = rr.ReadSimpleString();
        if (!response.Equals("OK", StringComparison.InvariantCultureIgnoreCase))
        {
            throw new Exception("not ok!");
        }
    }
    Console.WriteLine("handshake 3/4");

    _ = Task.Run(async () => await HandleClient(myMaster, true));
}

void FinalizeHandshake(ref RedisReader? rr, NetworkStream ns, byte[] buffer)
{
    Console.WriteLine("finalizing handshake");

    RedisWriter rw = new(ns);
    rw.WriteStringArray(["PSYNC", "?", "-1"]);
    {
        rw.Flush();
        Task.Delay(1000); // giving time to send all requests at once
        rr = ReadNetwork(ns, buffer);
        object last = rr.ReadAny(); // recieving FULLRESYNC here
        Console.WriteLine("final handshake message recieved:");
        if (last is object[] ar)
        {
            foreach (object o in ar) 
            {
                Console.WriteLine(o.ToString());
            }
        }
        else
        {
            Console.WriteLine(last);
        }
        Console.WriteLine("(end of final handshake message)");
        rr.SkipRDB();
    }

    Console.WriteLine("handshake 4/4");
}
