using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Cysharp.Threading.Tasks;

namespace MessagePipe.Interprocess.Workers
{
    // TODO:TCP STREAM AND READ

    public sealed class SocketTcpServer : IDisposable
    {
        const int MaxConnections = 0x7fffffff;

        readonly Socket socket;

        SocketTcpServer(AddressFamily addressFamily, ProtocolType protocolType, int? sendBufferSize, int? recvBufferSize)
        {
            socket = new Socket(addressFamily, SocketType.Stream, protocolType);
            if(sendBufferSize.HasValue)
            {
                socket.SendBufferSize = sendBufferSize.Value;
            }
            if(recvBufferSize.HasValue)
            {
                socket.ReceiveBufferSize = recvBufferSize.Value;
            }
        }

        public static SocketTcpServer Listen(string host, int port)
        {
            var ip = new IPEndPoint(IPAddress.Parse(host), port);
            var server = new SocketTcpServer(ip.AddressFamily, ProtocolType.Tcp, null, null);

            server.socket.Bind(ip);
            server.socket.Listen(MaxConnections);
            return server;
        }

#if NET5_0_OR_GREATER
        /// <summary>
        /// create TCP unix domain socket server and listen
        /// </summary>
        /// <param name="domainSocketPath">path to unix domain socket</param>
        /// <param name="recvBufferSize">socket's receive buffer size</param>
        /// <param name="sendBufferSize">socket's send buffer size</param>
        /// <exception cref="SocketException">unix domain socket not supported or socket already exists</exception>
        /// <returns>TCP unix domain socket server</returns>
        public static SocketTcpServer ListenUds(string domainSocketPath, int? sendBufferSize = null, int? recvBufferSize = null)
        {
            var server = new SocketTcpServer(AddressFamily.Unix, ProtocolType.IP, sendBufferSize, recvBufferSize);
            server.socket.Bind(new UnixDomainSocketEndPoint(domainSocketPath));
            server.socket.Listen(MaxConnections);
            return server;
        }
#endif

        // SocketTcpServer.cs - StartAcceptLoopAsync メソッドの修正
        [Preserve]
        public async void StartAcceptLoopAsync(Action<SocketTcpClient> onAccept, CancellationToken cancellationToken)
        {
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[TCP SERVER] StartAcceptLoopAsync called");
            #endif
            
            while (!cancellationToken.IsCancellationRequested)
            {
                Socket remote = default;
                try
                {
                    #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                    UnityEngine.Debug.Log($"[TCP SERVER] Waiting for client connection...");
                    #endif
                    
                    remote = await socket.AcceptAsync();
                    
                    #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                    UnityEngine.Debug.Log($"[TCP SERVER] Client connected: {remote.RemoteEndPoint}");
                    #endif
                }
                catch (Exception ex)
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                        UnityEngine.Debug.Log($"[TCP SERVER] Accept loop canceled");
                        #endif
                        return;
                    }
                    
                    if (ex is ObjectDisposedException)
                    {
                        #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                        UnityEngine.Debug.Log($"[TCP SERVER] Socket was disposed, ending accept loop");
                        #endif
                        return;
                    }
                    
                    #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                    UnityEngine.Debug.LogError($"[TCP SERVER] Error accepting client: {ex.Message}\n{ex.StackTrace}");
                    #endif
                    
                    // 短い遅延を入れて再試行（サーバーが一時的に応答しない場合への対応）
                    await Task.Delay(1000, cancellationToken).ConfigureAwait(false);
                    continue;
                }
                
                if (remote != null)
                {
                    try
                    {
                        // 接続を受け入れコールバックを呼び出し
                        onAccept(new SocketTcpClient(remote));
                    }
                    catch (Exception ex)
                    {
                        #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                        UnityEngine.Debug.LogError($"[TCP SERVER] Error in onAccept callback: {ex.Message}\n{ex.StackTrace}");
                        #endif
                        
                        // コールバックでエラーが発生しても接続ループは継続
                    }
                }
            }
        }

        public void Dispose()
        {
            socket.Dispose();
        }
    }

    public sealed class SocketTcpClient : IDisposable
    {
        readonly Socket socket;

        SocketTcpClient(AddressFamily addressFamily, ProtocolType protocolType)
        {
            socket = new Socket(addressFamily, SocketType.Stream, protocolType);
        }

        internal SocketTcpClient(Socket socket)
        {
            this.socket = socket;
        }

// SocketTcpClient.cs の Connect メソッド修正
        [Preserve]
        public static SocketTcpClient Connect(string host, int port)
        {
            try
            {
#if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.Log($"[TCP CLIENT] Connecting to {host}:{port}");
#endif
        
                // IPアドレスを解析
                var ip = new IPEndPoint(IPAddress.Parse(host), port);
                var client = new SocketTcpClient(ip.AddressFamily, ProtocolType.Tcp);
        
                // Connect呼び出しでブロック
                client.socket.Connect(ip);
        
#if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.Log($"[TCP CLIENT] Connected successfully to {host}:{port}");
#endif
        
                return client;
            }
            catch (Exception ex)
            {
#if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.LogError($"[TCP CLIENT] Connect error: {ex.Message}\n{ex.StackTrace}");
#endif
                throw;
            }
        }

        public static SocketTcpClient ConnectWithLocalEndpoint(string host, int port)
        {
            try
            {
                // 既存のメソッドを使用して接続
                return Connect(host, port);
            }
            catch (Exception ex)
            {
                UnityEngine.Debug.LogError($"[TCP DEBUG] ConnectWithLocalEndpoint error: {ex.Message}\n{ex.StackTrace}");
                throw;
            }
        }
        
#if NET5_0_OR_GREATER
        /// <summary>
        /// create TCP unix domain socket client and connect to server
        /// </summary>
        /// <param name="domainSocketPath">path to unix domain socket</param>
        /// <exception cref="SocketException">unix domain socket not supported or server does not listen</exception>
        /// <returns>TCP socket client.</returns>
        public static SocketTcpClient ConnectUds(string domainSocketPath)
        {
            try
            {
                var client = new SocketTcpClient(AddressFamily.Unix, ProtocolType.IP);
                client.socket.Connect(new UnixDomainSocketEndPoint(domainSocketPath));
                return client;
            }
            catch (Exception ex)
            {
                UnityEngine.Debug.LogError($"[TCP DEBUG] ConnectUds error: {ex.Message}\n{ex.StackTrace}");
                throw;
            }
        }
#endif
        public async UniTask<int> ReceiveAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
#if NET5_0_OR_GREATER
            var xs = new ArraySegment<byte>(buffer, offset, count);
            var i = await socket.ReceiveAsync(xs, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            return i;
#else
            var tcs = new UniTaskCompletionSource<int>();

            socket.BeginReceive(buffer, offset, count, SocketFlags.None, x =>
            {
                int i;
                try
                {
                    i = socket.EndReceive(x);
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                    return;
                }
                tcs.TrySetResult(i);
            }, null);

            return await tcs.Task;
#endif
        }

        public UniTask<int> SendAsync(byte[] buffer, CancellationToken cancellationToken = default)
        {
#if NET5_0_OR_GREATER
            return socket.SendAsync(buffer, SocketFlags.None, cancellationToken);
#else
            var tcs = new UniTaskCompletionSource<int>();
            socket.BeginSend(buffer, 0, buffer.Length, SocketFlags.None, x =>
             {
                 int i;
                 try
                 {
                     i = socket.EndSend(x);
                 }
                 catch (Exception ex)
                 {
                     tcs.TrySetException(ex);
                     return;
                 }
                 tcs.TrySetResult(i);
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