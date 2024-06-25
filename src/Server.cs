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
    while (client.Connected)
    {
        using var ms = new MemoryStream();
        await ns.CopyToAsync(ms);
        ms.Position = 0;
        object[] request = (object[])RedisSerial.ReadAny(ms);

        switch (request[0])
        {
            case "PING":
                RedisSerial.WriteSimpleString(ns, "PONG");
                break;
            case "ECHO":
                RedisSerial.WriteSimpleString(ns, (string)request[1]);
                break;
        }
    }
}