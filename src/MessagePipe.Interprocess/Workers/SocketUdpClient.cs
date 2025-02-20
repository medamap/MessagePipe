using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Cysharp.Threading.Tasks;

namespace MessagePipe.Interprocess.Workers
{
    internal sealed class SocketUdpServer : IDisposable
    {
        const int MinBuffer = 4096;
        readonly Socket socket;
        readonly byte[] buffer;

        SocketUdpServer(int bufferSize, AddressFamily addressFamily, ProtocolType protocolType)
        {
            socket = new Socket(addressFamily, SocketType.Dgram, protocolType);
            socket.ReceiveBufferSize = bufferSize;
            buffer = new byte[Math.Max(bufferSize, MinBuffer)];
        }

        public static SocketUdpServer Bind(int port, int bufferSize)
        {
            var server = new SocketUdpServer(bufferSize, AddressFamily.InterNetwork, ProtocolType.Udp);
            server.socket.Bind(new IPEndPoint(IPAddress.Any, port));
            return server;
        }

#if NET5_0_OR_GREATER
        public static SocketUdpServer BindUds(string domainSocketPath, int bufferSize)
        {
            var server = new SocketUdpServer(bufferSize, AddressFamily.Unix, ProtocolType.IP);
            server.socket.Bind(new UnixDomainSocketEndPoint(domainSocketPath));
            return server;
        }
#endif

        public async UniTask<ReadOnlyMemory<byte>> ReceiveAsync(CancellationToken cancellationToken)
        {
#if NET5_0_OR_GREATER
            int i = await socket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            return buffer.AsMemory(0, i);
#else
            var tcs = new UniTaskCompletionSource<ReadOnlyMemory<byte>>();
            socket.BeginReceive(buffer, 0, buffer.Length, SocketFlags.None, ar =>
            {
                int i;
                try { i = socket.EndReceive(ar); }
                catch (Exception ex) { tcs.TrySetException(ex); return; }
                tcs.TrySetResult(buffer.AsMemory(0, i));
            }, null);
            return await tcs.Task;
#endif
        }

        public void Dispose()
        {
            socket.Dispose();
        }
    }

    internal sealed class SocketUdpClient : IDisposable
    {
        const int MinBuffer = 4096;
        readonly Socket socket;
        readonly byte[] buffer;
        // 送信先エンドポイント。Broadcast送信の場合は、ここに保持して SendTo() で使用する
        readonly EndPoint remoteEndPoint;
        // ブロードキャスト送信の場合は true、通常の接続なら false
        readonly bool useSendTo;

        SocketUdpClient(int bufferSize, AddressFamily addressFamily, ProtocolType protocolType, EndPoint remoteEndPoint, bool useSendTo)
        {
            socket = new Socket(addressFamily, SocketType.Dgram, protocolType);
            socket.SendBufferSize = bufferSize;
            buffer = new byte[Math.Max(bufferSize, MinBuffer)];
            this.remoteEndPoint = remoteEndPoint;
            this.useSendTo = useSendTo;
        }

        public static SocketUdpClient Connect(string host, int port, int bufferSize)
        {
            var ipaddr = IPAddress.Parse(host);
            bool isBroadcast = ipaddr.Equals(IPAddress.Broadcast) || ipaddr.ToString() == "255.255.255.255";
            if (isBroadcast)
            {
                // ブロードキャストの場合、Connect() は行わず、送信先エンドポイントを保持し、Broadcastオプションを有効にする
                var endPoint = new IPEndPoint(ipaddr, port);
                var client = new SocketUdpClient(bufferSize, ipaddr.AddressFamily, ProtocolType.Udp, endPoint, true);
                client.socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Broadcast, true);
                return client;
            }
            else
            {
                // 通常はConnect() を呼び出して既定の送信先を設定
                var endPoint = new IPEndPoint(ipaddr, port);
                var client = new SocketUdpClient(bufferSize, ipaddr.AddressFamily, ProtocolType.Udp, endPoint, false);
                client.socket.Connect(endPoint);
                return client;
            }
        }

#if NET5_0_OR_GREATER
        public static SocketUdpClient ConnectUds(string domainSocketPath, int bufferSize)
        {
            var endPoint = new UnixDomainSocketEndPoint(domainSocketPath);
            var client = new SocketUdpClient(bufferSize, AddressFamily.Unix, ProtocolType.IP, endPoint, false);
            client.socket.Connect(endPoint);
            return client;
        }
#endif

        public UniTask<int> SendAsync(byte[] data, CancellationToken cancellationToken = default)
        {
#if NET5_0_OR_GREATER
            if (useSendTo)
            {
                // ブロードキャストの場合は SendTo を使用
                return socket.SendToAsync(data, SocketFlags.None, remoteEndPoint, cancellationToken);
            }
            else
            {
                return socket.SendAsync(data, SocketFlags.None, cancellationToken);
            }
#else
            var tcs = new UniTaskCompletionSource<int>();
            if (useSendTo)
            {
                socket.BeginSendTo(data, 0, data.Length, SocketFlags.None, remoteEndPoint, ar =>
                {
                    try { tcs.TrySetResult(socket.EndSend(ar)); }
                    catch (Exception ex) { tcs.TrySetException(ex); }
                }, null);
            }
            else
            {
                socket.BeginSend(data, 0, data.Length, SocketFlags.None, ar =>
                {
                    try { tcs.TrySetResult(socket.EndSend(ar)); }
                    catch (Exception ex) { tcs.TrySetException(ex); }
                }, null);
            }
#if !UNITY_2018_3_OR_NEWER
            return new UniTask<int>(tcs.Task);
#else
            return tcs.Task;
#endif
#endif
        }

        public void Dispose()
        {
            socket.Dispose();
        }
    }
}
