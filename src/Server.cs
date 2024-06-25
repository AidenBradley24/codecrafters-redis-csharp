using System.Net;
using System.Net.Sockets;
using System.Text;

TcpListener server = new(IPAddress.Any, 6379);
server.Start();
TcpClient client = server.AcceptTcpClient();

NetworkStream ns = client.GetStream();
byte[] buffer = new byte[1024];
ns.Read(buffer);

if (Encoding.ASCII.GetString(buffer).Contains("PING"))
{
    ns.Write(Encoding.ASCII.GetBytes("+PONG\r\n"));
}