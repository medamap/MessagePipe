using MessagePack;
using MessagePipe.Interprocess.Internal;
#if !UNITY_2018_3_OR_NEWER
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Channels;
#endif
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Cysharp.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace MessagePipe.Interprocess.Workers
{
    [Preserve]
    public sealed class TcpWorker : IDisposable
    {
        readonly IServiceProvider provider;
        readonly CancellationTokenSource cancellationTokenSource;
        readonly IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher;
        readonly MessagePipeInterprocessOptions options;

        // Channel is used from publisher for thread safety of write packet
        int initializedServer = 0;
        Lazy<SocketTcpServer> server;
        Channel<TcpMessageContainer> channel;

        int initializedClient = 0;
        Lazy<SocketTcpClient> client;

        // 接続プール
        private TcpConnectionPool connectionPool;

        // request-response
        int messageId = 0;
        ConcurrentDictionary<int, UniTaskCompletionSource<IInterprocessValue>> responseCompletions = new ConcurrentDictionary<int, UniTaskCompletionSource<IInterprocessValue>>();

        // create from DI
        [Preserve]
        public TcpWorker(IServiceProvider provider, MessagePipeInterprocessTcpOptions options, IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher)
        {
            this.provider = provider;
            this.cancellationTokenSource = new CancellationTokenSource();
            this.options = options;
            this.publisher = publisher;

            this.server = new Lazy<SocketTcpServer>(() => 
            {
                return SocketTcpServer.Listen(options.Host, options.Port);
            });

            // クライアント初期化を変更
            this.client = new Lazy<SocketTcpClient>(() => 
            {
                try
                {
                    // ローカルホストへの接続を作成
                    return SocketTcpClient.Connect("127.0.0.1", options.Port);
                }
                catch (Exception ex)
                {
                    UnityEngine.Debug.LogError($"[TCP DEBUG] Client initialization failed: {ex.Message}\n{ex.StackTrace}");
                    throw;
                }
            });

#if !UNITY_2018_3_OR_NEWER
            this.channel = Channel.CreateUnbounded<TcpMessageContainer>(new UnboundedChannelOptions()
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true
            });
#else
            this.channel = Channel.CreateSingleConsumerUnbounded<TcpMessageContainer>();
#endif

            // 接続プールの初期化
            InitializeConnectionPool();

            if (options.HostAsServer != null && options.HostAsServer.Value)
            {
                StartReceiver();
            }
        }

        #if NET5_0_OR_GREATER
        [Preserve]
        public TcpWorker(IServiceProvider provider, MessagePipeInterprocessTcpUdsOptions options, IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher)
        {
            this.provider = provider;
            this.cancellationTokenSource = new CancellationTokenSource();
            this.options = options;
            this.publisher = publisher;

            this.server = new Lazy<SocketTcpServer>(() => 
            {
                return SocketTcpServer.ListenUds(options.SocketPath);
            });

            // クライアント初期化を変更
            this.client = new Lazy<SocketTcpClient>(() => 
            {
                try
                {
                    return SocketTcpClient.ConnectUds(options.SocketPath);
                }
                catch (Exception ex)
                {
                    UnityEngine.Debug.LogError($"[TCP DEBUG] Client initialization failed: {ex.Message}\n{ex.StackTrace}");
                    throw;
                }
            });

#if !UNITY_2018_3_OR_NEWER
            this.channel = Channel.CreateUnbounded<TcpMessageContainer>(new UnboundedChannelOptions()
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true
            });
#else
            this.channel = Channel.CreateSingleConsumerUnbounded<TcpMessageContainer>();
#endif

            // 接続プールの初期化
            InitializeConnectionPool();

            if (options.HostAsServer != null && options.HostAsServer.Value)
            {
                StartReceiver();
            }
        }
#endif

        /// <summary>
        /// 接続プールを初期化
        /// </summary>
        private void InitializeConnectionPool()
        {
            TimeSpan connectionTimeout = TimeSpan.FromMinutes(5);
            TimeSpan cleanupInterval = TimeSpan.FromMinutes(1);

            if (options is MessagePipeInterprocessTcpExtendedOptions extOptions)
            {
                connectionTimeout = extOptions.IdleConnectionTimeout;
                cleanupInterval = extOptions.ConnectionPoolCleanupInterval;
            }

            connectionPool = new TcpConnectionPool(options, connectionTimeout, cleanupInterval);
        }

        /// <summary>
        /// メッセージをシリアライズ
        /// </summary>
        public byte[] SerializeMessage<TKey, TMessage>(TKey key, TMessage message)
        {
            return MessageBuilder.BuildPubSubMessage(key, message, options.MessagePackSerializerOptions);
        }

        /// <summary>
        /// 従来のPublishメソッド
        /// </summary>
        public void Publish<TKey, TMessage>(TKey key, TMessage message)
        {
            // デフォルトの送信先を使用
            PublishToTarget(key, message, null, null);
        }

        /// <summary>
        /// 送信先を指定してメッセージを発行
        /// </summary>
        public void PublishToTarget<TKey, TMessage>(TKey key, TMessage message, string targetAddress, int? targetPort = null)
        {
#if MESSAGEPIPE_TCP_SEND_DEBUG
    UnityEngine.Debug.Log($"[TCP SEND] PublishToTarget - Key: {key}, Message: {message}, TargetAddress: {targetAddress}, TargetPort: {targetPort}");
#endif
            if (Interlocked.Increment(ref initializedClient) == 1) // first incr, channel not yet started
            {
                try
                {
#if MESSAGEPIPE_TCP_SEND_DEBUG
            UnityEngine.Debug.Log($"[TCP SEND] Initializing client for the first time");
#endif
                    _ = client.Value; // init
                    RunPublishLoop();
                }
                catch (Exception ex)
                {
#if MESSAGEPIPE_TCP_SEND_DEBUG
            UnityEngine.Debug.LogError($"[TCP SEND] Client initialization failed: {ex.Message}\n{ex.StackTrace}");
#endif
                    // クライアント初期化に失敗した場合
                    Interlocked.Exchange(ref initializedClient, 0); // リセット
            
                    // 拡張オプションの場合はエラー無視設定を確認
                    bool ignoreErrors = options is MessagePipeInterprocessTcpExtendedOptions extOptions1 && extOptions1.IgnoreConnectErrors;
                    if (ignoreErrors)
                    {
                        options.UnhandledErrorHandler?.Invoke("TCP client initialization failed, but continuing due to IgnoreConnectErrors option.", ex);
                        return;
                    }
            
                    // それ以外は例外を再スロー
                    throw;
                }
            }

            var buffer = MessageBuilder.BuildPubSubMessage(key, message, options.MessagePackSerializerOptions);
    
            // メッセージコンテナを作成して送信キューに追加
            var container = new TcpMessageContainer
            {
                Data = buffer,
                ToAddress = targetAddress,
                Port = targetPort
            };
    
            // 拡張オプションの場合は追加設定を適用
            if (options is MessagePipeInterprocessTcpExtendedOptions extOptions2)
            {
                container.RetryCount = extOptions2.MaxRetryCount;
                container.Timeout = extOptions2.SendTimeout;
            }
    
#if MESSAGEPIPE_TCP_SEND_DEBUG
    UnityEngine.Debug.Log($"[TCP SEND] Adding message to channel - ID: {container.MessageId}, ToAddress: {container.ToAddress}, Port: {container.Port}");
#endif
            channel.Writer.TryWrite(container);
        }

        /// <summary>
        /// Fluent APIを使用するためのファクトリーメソッド
        /// </summary>
        public FluentTcpPublisher CreatePublisher()
        {
            return new FluentTcpPublisher(this, connectionPool, options);
        }

        public async UniTask<TResponse> RequestAsync<TRequest, TResponse>(TRequest request, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref initializedClient) == 1) // first incr, channel not yet started
            {
                _ = client.Value; // init
                RunPublishLoop();
            }

            var mid = Interlocked.Increment(ref messageId);
            var tcs = new UniTaskCompletionSource<IInterprocessValue>();
            responseCompletions[mid] = tcs;
            var buffer = MessageBuilder.BuildRemoteRequestMessage(typeof(TRequest), typeof(TResponse), mid, request, options.MessagePackSerializerOptions);
            
            // メッセージコンテナを作成して送信キューに追加
            var container = new TcpMessageContainer
            {
                Data = buffer,
                ToAddress = null, // デフォルトの送信先を使用
                Port = null
            };
            
            // 拡張オプションの場合は追加設定を適用
            if (options is MessagePipeInterprocessTcpExtendedOptions extOptions)
            {
                container.RetryCount = extOptions.MaxRetryCount;
                container.Timeout = extOptions.SendTimeout;
            }
            
            channel.Writer.TryWrite(container);
            
            var memoryValue = await tcs.Task.ConfigureAwait(false);
            return MessagePackSerializer.Deserialize<TResponse>(memoryValue.ValueMemory, options.MessagePackSerializerOptions);
        }

        // Send packet to tcp socket from publisher
        async void RunPublishLoop()
        {
#if MESSAGEPIPE_TCP_SEND_DEBUG
            UnityEngine.Debug.Log($"[TCP SEND] RunPublishLoop started");
#endif
            var reader = channel.Reader;
            var token = cancellationTokenSource.Token;
            
            // デフォルトクライアントの初期化と受信ループの開始をスキップ
            // var tcpClient = client.Value;
            // RunReceiveLoop(tcpClient);

            while (await reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
#if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.Log($"[TCP SEND] WaitToReadAsync completed, processing messages");
#endif
                while (reader.TryRead(out var item))
                {
#if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.Log($"[TCP SEND] Processing message - ID: {item.MessageId}, ToAddress: {item.ToAddress}, Port: {item.Port}");
#endif
                    try
                    {
                        // 送信先が指定されている場合は接続プールから取得または作成
                        SocketTcpClient targetClient;
                        if (!string.IsNullOrEmpty(item.ToAddress) && item.Port.HasValue)
                        {
#if MESSAGEPIPE_TCP_SEND_DEBUG
                            UnityEngine.Debug.Log($"[TCP SEND] Getting connection from pool - Address: {item.ToAddress}, Port: {item.Port}");
#endif
                            targetClient = await connectionPool.GetOrCreateConnectionAsync(item.ToAddress, item.Port.Value, token);
                            if (targetClient == null)
                            {
#if MESSAGEPIPE_TCP_SEND_DEBUG
                                UnityEngine.Debug.LogError($"[TCP SEND] Failed to get connection to {item.ToAddress}:{item.Port}");
#endif
                                // 接続の取得に失敗した場合
                                bool ignoreErrors = options is MessagePipeInterprocessTcpExtendedOptions extOptions && extOptions.IgnoreConnectErrors;
                                if (ignoreErrors)
                                {
                                    options.UnhandledErrorHandler?.Invoke($"Failed to get connection to {item.ToAddress}:{item.Port}, but continuing due to IgnoreConnectErrors option.", null);
                                    continue; // 次のメッセージへ
                                }
                                else
                                {
                                    throw new InvalidOperationException($"Failed to get connection to {item.ToAddress}:{item.Port}");
                                }
                            }
                        }
                        else
                        {
#if MESSAGEPIPE_TCP_SEND_DEBUG
                            UnityEngine.Debug.Log($"[TCP SEND] No specific address/port, creating connection to localhost");
#endif
                            // デフォルトの接続を使用する代わりに、ローカルホストへの接続を作成
                            targetClient = await connectionPool.GetOrCreateConnectionAsync("127.0.0.1", (options as MessagePipeInterprocessTcpOptions)?.Port ?? 37564, token);
                            if (targetClient == null)
                            {
#if MESSAGEPIPE_TCP_SEND_DEBUG
                                UnityEngine.Debug.LogError($"[TCP SEND] Failed to get connection to localhost");
#endif
                                bool ignoreErrors = options is MessagePipeInterprocessTcpExtendedOptions extOptions && extOptions.IgnoreConnectErrors;
                                if (ignoreErrors)
                                {
                                    options.UnhandledErrorHandler?.Invoke($"Failed to get connection to localhost, but continuing due to IgnoreConnectErrors option.", null);
                                    continue; // 次のメッセージへ
                                }
                                else
                                {
                                    throw new InvalidOperationException($"Failed to get connection to localhost");
                                }
                            }
                        }
                        
#if MESSAGEPIPE_TCP_SEND_DEBUG
                        UnityEngine.Debug.Log($"[TCP SEND] Sending data - Length: {item.Data.Length} bytes");
#endif
                        await targetClient.SendAsync(item.Data, token).ConfigureAwait(false);
                        
#if MESSAGEPIPE_TCP_SEND_DEBUG
                        UnityEngine.Debug.Log($"[TCP SEND] Send completed successfully - ID: {item.MessageId}");
#endif
                        // 送信成功
                        item.State = TcpMessageState.Completed;
                        item.CompletionCallback?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        if (ex is OperationCanceledException) return;
                        if (token.IsCancellationRequested) return;

#if MESSAGEPIPE_TCP_SEND_DEBUG
                        UnityEngine.Debug.LogError($"[TCP SEND] Error sending message - ID: {item.MessageId}, Error: {ex.Message}\n{ex.StackTrace}");
#endif
                        // エラーコールバックが設定されている場合は呼び出し
                        if (item.ErrorCallback != null)
                        {
                            item.ErrorCallback(ex);
                            continue; // 次のメッセージへ
                        }
                        
                        // 送信エラーの処理
                        bool ignoreErrors = options is MessagePipeInterprocessTcpExtendedOptions extOptions && extOptions.IgnoreSendErrors;
                        if (ignoreErrors)
                        {
                            options.UnhandledErrorHandler?.Invoke("Network send error, continuing due to IgnoreSendErrors option.", ex);
                            continue; // 次のメッセージへ
                        }

                        // network error, terminate.
                        options.UnhandledErrorHandler("network error, publish loop will terminate." + Environment.NewLine, ex);
                        return;
                    }
                }
            }
        }

        public void StartReceiver()
        {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[TCP RECEIVE] StartReceiver called");
#endif
            if (Interlocked.Increment(ref initializedServer) == 1) // first incr, channel not yet started
            {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.Log($"[TCP RECEIVE] Initializing server for the first time");
#endif
                var s = server.Value; // init
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                var tcpOptions = options as MessagePipeInterprocessTcpOptions;
                UnityEngine.Debug.Log($"[TCP RECEIVE] Server initialized - Host: {tcpOptions?.Host}, Port: {tcpOptions?.Port}, HostAsServer: {tcpOptions?.HostAsServer}");
#endif
                s.StartAcceptLoopAsync(RunReceiveLoop, cancellationTokenSource.Token);
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.Log($"[TCP RECEIVE] StartAcceptLoopAsync called");
#endif

                // クライアント側の受信ループも開始（サーバーとは別に）
                if (client.IsValueCreated)
                {
                    try
                    {
                        RunReceiveLoop(client.Value);
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                        UnityEngine.Debug.Log($"[TCP RECEIVE] Client receive loop started");
#endif
                    }
                    catch (Exception ex)
                    {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                        UnityEngine.Debug.LogError($"[TCP RECEIVE] Error starting client receive loop: {ex.Message}\n{ex.StackTrace}");
#endif
                        // エラーを無視
                    }
                }
            }
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            else
            {
                UnityEngine.Debug.Log($"[TCP RECEIVE] Server already initialized, initializedServer = {initializedServer}");
            }
#endif
        }


        // Receive from tcp socket and push value to subscribers.
        async void RunReceiveLoop(SocketTcpClient client)
        {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
    UnityEngine.Debug.Log($"[TCP RECEIVE] RunReceiveLoop started for client: {client.GetHashCode()}");
#endif
            var token = cancellationTokenSource.Token;
            var buffer = new byte[65536];
            ReadOnlyMemory<byte> readBuffer = Array.Empty<byte>();
            while (!token.IsCancellationRequested)
            {
                ReadOnlyMemory<byte> value = Array.Empty<byte>();
                try
                {
                    if (readBuffer.Length == 0)
                    {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.Log($"[TCP RECEIVE] Waiting to receive data...");
#endif
                        var readLen = await client.ReceiveAsync(buffer, 0, buffer.Length, token).ConfigureAwait(false);
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.Log($"[TCP RECEIVE] Received {readLen} bytes");
#endif
                        if (readLen == 0)
                        {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                    UnityEngine.Debug.Log($"[TCP RECEIVE] End of stream (disconnect)");
#endif
                            return; // end of stream(disconnect)
                        }
                        readBuffer = buffer.AsMemory(0, readLen);
                    }
                    else if (readBuffer.Length < 4) // rare case
                    {
                        var readLen = await client.ReceiveAsync(buffer, 0, buffer.Length, token).ConfigureAwait(false);
                        if (readLen == 0) return;
                        var newBuffer = new byte[readBuffer.Length + readLen];
                        readBuffer.CopyTo(newBuffer);
                        buffer.AsSpan(readLen).CopyTo(newBuffer.AsSpan(readBuffer.Length));
                        readBuffer = newBuffer;
                    }

                    var messageLen = MessageBuilder.FetchMessageLength(readBuffer.Span);
                    if (readBuffer.Length == (messageLen + 4)) // just size
                    {
                        value = readBuffer.Slice(4, messageLen); // skip length header
                        readBuffer = Array.Empty<byte>();
                        goto PARSE_MESSAGE;
                    }
                    else if (readBuffer.Length > (messageLen + 4)) // over size
                    {
                        value = readBuffer.Slice(4, messageLen);
                        readBuffer = readBuffer.Slice(messageLen + 4);
                        goto PARSE_MESSAGE;
                    }
                    else // needs to read more
                    {
                        var readLen = readBuffer.Length;
                        if (readLen < (messageLen + 4))
                        {
                            if (readBuffer.Length != buffer.Length)
                            {
                                var newBuffer = new byte[buffer.Length];
                                readBuffer.CopyTo(newBuffer);
                                buffer = newBuffer;
                            }

                            if (buffer.Length < messageLen + 4)
                            {
                                Array.Resize(ref buffer, messageLen + 4);
                            }
                        }
                        var remain = messageLen - (readLen - 4);
                        await ReadFullyAsync(buffer, client, readLen, remain, token).ConfigureAwait(false);
                        value = buffer.AsMemory(4, messageLen);
                        readBuffer = Array.Empty<byte>();
                        goto PARSE_MESSAGE;
                    }
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException) return;
                    if (token.IsCancellationRequested) return;

#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.LogError($"[TCP RECEIVE] Network error: {ex.Message}\n{ex.StackTrace}");
#endif
                    // network error, terminate.
                    options.UnhandledErrorHandler("network error, receive loop will terminate." + Environment.NewLine, ex);
                    return;
                }
            PARSE_MESSAGE:
                try
                {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.Log($"[TCP RECEIVE] Parsing message, length: {value.Length} bytes");
#endif
                    var message = MessageBuilder.ReadPubSubMessage(value.ToArray()); // can avoid copy?
                    switch (message.MessageType)
                    {
                        case MessageType.PubSub:
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                            UnityEngine.Debug.Log($"[TCP RECEIVE] Received PubSub message");
                            // メッセージの内容をより詳細に表示
                            try {
                                var keyString = System.Text.Encoding.UTF8.GetString(message.KeyMemory.Span);
                                UnityEngine.Debug.Log($"[TCP RECEIVE] PubSub message key: {keyString}");
                            } catch {
                                UnityEngine.Debug.Log($"[TCP RECEIVE] Could not decode key as string");
                            }
#endif
                            publisher.Publish(message, message, CancellationToken.None);
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                            UnityEngine.Debug.Log($"[TCP RECEIVE] Published message to internal subscribers");
#endif
                            break;
                        case MessageType.RemoteRequest:
                            {
                                // NOTE: should use without reflection(Expression.Compile)
                                var header = Deserialize<RequestHeader>(message.KeyMemory, options.MessagePackSerializerOptions);
                                var (mid, reqTypeName, resTypeName) = (header.MessageId, header.RequestType, header.ResponseType);
                                byte[] resultBytes;
                                try
                                {
                                    var t = AsyncRequestHandlerRegistory.Get(reqTypeName, resTypeName);
                                    var interfaceType = t.GetInterfaces().Where(x => x.IsGenericType && x.Name.StartsWith("IAsyncRequestHandler"))
                                        .First(x => x.GetGenericArguments().Any(y => y.FullName == header.RequestType));
                                    var coreInterfaceType = t.GetInterfaces().Where(x => x.IsGenericType && x.Name.StartsWith("IAsyncRequestHandlerCore"))
                                        .First(x => x.GetGenericArguments().Any(y => y.FullName == header.RequestType));
                                    var service = provider.GetRequiredService(interfaceType); // IAsyncRequestHandler<TRequest,TResponse>
                                    var genericArgs = interfaceType.GetGenericArguments(); // [TRequest, TResponse]
                                    // Unity IL2CPP does not work(can not invoke nongenerics MessagePackSerializer)
                                    var request = MessagePackSerializer.Deserialize(genericArgs[0], message.ValueMemory, options.MessagePackSerializerOptions);
                                    var responseTask = coreInterfaceType.GetMethod("InvokeAsync").Invoke(service, new[] { request, CancellationToken.None });
#if !UNITY_2018_3_OR_NEWER
                                    var task = typeof(UniTask<>).MakeGenericType(genericArgs[1]).GetMethod("AsTask").Invoke(responseTask, null);
#else
                                    var asTask = typeof(UniTaskExtensions).GetMethods().First(x => x.IsGenericMethod && x.Name == "AsTask")
                                        .MakeGenericMethod(genericArgs[1]);
                                    var task = asTask.Invoke(null, new[] { responseTask });
#endif
                                    await ((System.Threading.Tasks.Task)task); // Task<T> -> Task
                                    var result = task.GetType().GetProperty("Result").GetValue(task);
                                    resultBytes = MessageBuilder.BuildRemoteResponseMessage(mid, genericArgs[1], result, options.MessagePackSerializerOptions);
                                }
                                catch (Exception ex)
                                {
                                    // NOTE: ok to send stacktrace?
                                    resultBytes = MessageBuilder.BuildRemoteResponseError(mid, ex.ToString(), options.MessagePackSerializerOptions);
                                }

                                await client.SendAsync(resultBytes).ConfigureAwait(false);
                            }
                            break;
                        case MessageType.RemoteResponse:
                        case MessageType.RemoteError:
                            {
                                var mid = Deserialize<int>(message.KeyMemory, options.MessagePackSerializerOptions);
                                if (responseCompletions.TryRemove(mid, out var tcs))
                                {
                                    if (message.MessageType == MessageType.RemoteResponse)
                                    {
                                        tcs.TrySetResult(message); // synchronous completion, use memory buffer immediately.
                                    }
                                    else
                                    {
                                        var errorMsg = MessagePackSerializer.Deserialize<string>(message.ValueMemory, options.MessagePackSerializerOptions);
                                        tcs.TrySetException(new RemoteRequestException(errorMsg));
                                    }
                                }
                            }
                            break;
                        default:
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                        UnityEngine.Debug.LogWarning($"[TCP RECEIVE] Unknown message type: {message.MessageType}");
#endif
                            break;
                    }
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException) continue;
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.LogError($"[TCP RECEIVE] Error processing message: {ex.Message}\n{ex.StackTrace}");
#endif
                    options.UnhandledErrorHandler("", ex);
                }
            }
        }

        // omajinai.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static T Deserialize<T>(ReadOnlyMemory<byte> buffer, MessagePackSerializerOptions options)
        {
            if (buffer.IsEmpty && MemoryMarshal.TryGetArray(buffer, out var segment))
            {
                buffer = segment;
            }
            return MessagePackSerializer.Deserialize<T>(buffer, options);
        }

        static async UniTask ReadFullyAsync(byte[] buffer, SocketTcpClient client, int index, int remain, CancellationToken token)
        {
            while (remain > 0)
            {
                var len = await client.ReceiveAsync(buffer, index, remain, token).ConfigureAwait(false);
                index += len;
                remain -= len;
            }
        }

        public void Dispose()
        {
            channel.Writer.TryComplete();

            cancellationTokenSource.Cancel();
            cancellationTokenSource.Dispose();

            if (server.IsValueCreated)
            {
                server.Value.Dispose();
            }

            if (client.IsValueCreated)
            {
                client.Value.Dispose();
            }
            
            connectionPool?.Dispose();
        }
    }
}
