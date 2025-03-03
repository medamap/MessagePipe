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
        // remoteEndPoint を保持して、ブロードキャスト送信時に SendTo() を利用する
        readonly EndPoint remoteEndPoint;
        readonly bool useSendTo;
        readonly AddressFamily addressFamily;
        readonly int port;

        SocketUdpClient(int bufferSize, AddressFamily addressFamily, ProtocolType protocolType, EndPoint remoteEndPoint, bool useSendTo, int port = 0)
        {
            socket = new Socket(addressFamily, SocketType.Dgram, protocolType);
            socket.SendBufferSize = bufferSize;
            buffer = new byte[Math.Max(bufferSize, MinBuffer)];
            this.remoteEndPoint = remoteEndPoint;
            this.useSendTo = useSendTo;
            this.addressFamily = addressFamily;
            this.port = port;
        }

        /// <summary>
        /// UDP クライアントの接続を行います。
        /// オプションとして subnetMask と networkAddress を指定でき、
        /// これらが設定されている場合は、ブロードキャストアドレスを計算して判定します。
        /// </summary>
        /// <param name="host">送信先ホスト（通常はIP文字列）</param>
        /// <param name="port">送信先ポート</param>
        /// <param name="bufferSize">バッファサイズ</param>
        /// <param name="subnetMask">サブネットマスク（例:"255.255.255.0"） ※任意</param>
        /// <param name="networkAddress">ネットワークアドレス（例:"192.168.1.0"） ※任意</param>
        /// <returns></returns>
        public static SocketUdpClient Connect(string host, int port, int bufferSize, string subnetMask = null, string networkAddress = null)
        {
            bool isBroadcast = false;
            IPAddress hostIP = IPAddress.Parse(host);
            // まず、ホストが "255.255.255.255" であればブロードキャスト
            if (hostIP.Equals(IPAddress.Broadcast) || host == "255.255.255.255")
            {
                isBroadcast = true;
            }
            // もしサブネット情報が与えられていれば、計算して判定
            else if (!string.IsNullOrEmpty(subnetMask) && !string.IsNullOrEmpty(networkAddress))
            {
                try
                {
                    var maskBytes = IPAddress.Parse(subnetMask).GetAddressBytes();
                    var networkBytes = IPAddress.Parse(networkAddress).GetAddressBytes();
                    if (maskBytes.Length == networkBytes.Length)
                    {
                        byte[] broadcastBytes = new byte[maskBytes.Length];
                        for (int i = 0; i < maskBytes.Length; i++)
                        {
                            // ブロードキャストアドレス = ネットワークアドレス OR (NOT サブネットマスク)
                            broadcastBytes[i] = (byte)(networkBytes[i] | (~maskBytes[i]));
                        }
                        var computedBroadcast = new IPAddress(broadcastBytes);
                        if (hostIP.Equals(computedBroadcast))
                        {
                            isBroadcast = true;
                        }
                    }
                }
                catch (Exception)
                {
                    // サブネット情報の解析に失敗した場合は、通常の接続とする
                    isBroadcast = false;
                }
            }

            if (isBroadcast)
            {
                // ブロードキャストの場合、Connect() を呼ばずに remoteEndPoint を保持する
                var broadcastIP = IPAddress.Broadcast; // 255.255.255.255
                var endpoint = new IPEndPoint(broadcastIP, port);
                var client = new SocketUdpClient(bufferSize, broadcastIP.AddressFamily, ProtocolType.Udp, endpoint, true, port);
                client.socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Broadcast, true);
                return client;
            }
            else
            {
                // 通常接続の場合
                var endpoint = new IPEndPoint(hostIP, port);
                var client = new SocketUdpClient(bufferSize, hostIP.AddressFamily, ProtocolType.Udp, endpoint, false, port);
                client.socket.Connect(endpoint);
                return client;
            }
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

        /// <summary>
        /// デフォルトのエンドポイントにデータを送信します
        /// </summary>
        public UniTask<int> SendAsync(byte[] data, CancellationToken cancellationToken = default)
        {
            return SendToEndpointAsync(data, remoteEndPoint, cancellationToken);
        }

        /// <summary>
        /// 指定されたアドレスにデータを送信します
        /// </summary>
        public UniTask<int> SendToAsync(byte[] data, string toAddress, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(toAddress))
            {
                return SendAsync(data, cancellationToken);
            }

            try
            {
                IPAddress targetIP = IPAddress.Parse(toAddress);
                bool isBroadcast = targetIP.Equals(IPAddress.Broadcast) || toAddress == "255.255.255.255";
                
                // ブロードキャストの場合はソケットオプションを設定
                if (isBroadcast)
                {
                    socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Broadcast, true);
                }
                
                var endpoint = new IPEndPoint(targetIP, port);
                return SendToEndpointAsync(data, endpoint, cancellationToken);
            }
            catch (Exception)
            {
                // アドレス解析に失敗した場合はデフォルトエンドポイントを使用
                return SendAsync(data, cancellationToken);
            }
        }

        /// <summary>
        /// 指定されたエンドポイントにデータを送信します
        /// </summary>
        private UniTask<int> SendToEndpointAsync(byte[] data, EndPoint endpoint, CancellationToken cancellationToken = default)
        {
#if NET5_0_OR_GREATER
            return socket.SendToAsync(data, SocketFlags.None, endpoint, cancellationToken);
#else
            var tcs = new UniTaskCompletionSource<int>();
            socket.BeginSendTo(data, 0, data.Length, SocketFlags.None, endpoint, ar =>
            {
                try { tcs.TrySetResult(socket.EndSendTo(ar)); }
                catch (Exception ex) { tcs.TrySetException(ex); }
            }, null);
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