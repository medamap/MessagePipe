using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Cysharp.Threading.Tasks;

namespace MessagePipe.Interprocess.Workers
{
    internal sealed class SocketUdpClient : IDisposable
    {
        const int MinBuffer = 4096;
        readonly Socket socket;
        readonly byte[] buffer;
        // 送信先エンドポイント。ブロードキャストの場合は常にこのエンドポイントを使用
        readonly EndPoint remoteEndPoint;
        // ブロードキャスト送信の場合は true を設定
        readonly bool useSendTo;

        SocketUdpClient(int bufferSize, AddressFamily addressFamily, ProtocolType protocolType, EndPoint remoteEndPoint, bool useSendTo)
        {
            socket = new Socket(addressFamily, SocketType.Dgram, protocolType);
            socket.SendBufferSize = bufferSize;
            buffer = new byte[Math.Max(bufferSize, MinBuffer)];
            this.remoteEndPoint = remoteEndPoint;
            this.useSendTo = useSendTo;
        }

        // 強制的にブロードキャストモードを使う実装
        public static SocketUdpClient Connect(string host, int port, int bufferSize)
        {
            // ブロードキャスト用に常に IPAddress.Broadcast を使用
            var broadcastIP = IPAddress.Broadcast; // 255.255.255.255
            var endpoint = new IPEndPoint(broadcastIP, port);
            var client = new SocketUdpClient(bufferSize, broadcastIP.AddressFamily, ProtocolType.Udp, endpoint, true);
            // ブロードキャスト送信を許可する
            client.socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Broadcast, true);
            return client;
        }

#if NET5_0_OR_GREATER
        public static SocketUdpClient ConnectUds(string domainSocketPath, int bufferSize)
        {
            var endpoint = new UnixDomainSocketEndPoint(domainSocketPath);
            var client = new SocketUdpClient(bufferSize, AddressFamily.Unix, ProtocolType.IP, endpoint, false);
            client.socket.Connect(endpoint);
            return client;
        }
#endif

        public UniTask<int> SendAsync(byte[] data, CancellationToken cancellationToken = default)
        {
#if NET5_0_OR_GREATER
            if (useSendTo)
            {
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
