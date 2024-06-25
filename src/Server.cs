using System.Net;
using System.Net.Sockets;

TcpListener server = new(IPAddress.Any, 6379);
server.Start();
server.AcceptSocket(); // wait for client
