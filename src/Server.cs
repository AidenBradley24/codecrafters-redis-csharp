using codecrafters_redis.src;
using System.Net;
using System.Net.Sockets;

TcpListener server = new(IPAddress.Any, 6379);
server.Start();

while (true)
{
    TcpClient client = server.AcceptTcpClient();
    _ = Task.Run(async () => await HandleClient(client));
}

static async Task HandleClient(TcpClient client)
{
    NetworkStream ns = client.GetStream();
    byte[] buffer = new byte[1024];
    while (client.Connected)
    {
        await ns.ReadAsync(buffer);
        using var ms = new MemoryStream(buffer);
        ms.Position = 0;
        object[] request = (object[])RedisSerial.ReadAny(ms);

        foreach (object obj in request)
        {
            Console.WriteLine(obj);
        }

        switch (request[0])
        {
            case "PING":
                RedisSerial.WriteSimpleString(ns, "PONG");
                break;
            case "ECHO":
                RedisSerial.WriteBulkString(ns, (string)request[1]);
                break;
        }
    }
}