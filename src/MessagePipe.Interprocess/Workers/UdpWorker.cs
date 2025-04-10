using MessagePipe.Interprocess.Internal;
using System;
using System.Threading;
using System.Net.Sockets;
#if !UNITY_2018_3_OR_NEWER
using System.Threading.Channels;
#else
using Cysharp.Threading.Tasks;
#endif

namespace MessagePipe.Interprocess.Workers
{
    // メッセージキューに入れるためのコンテナクラス
    internal class UdpMessageContainer
    {
        public byte[] Data { get; set; }
        public string ToAddress { get; set; }
        public int? Port { get; set; } // ポート番号を追加
    }

    [Preserve]
    public sealed class UdpWorker : IDisposable
    {
        readonly CancellationTokenSource cancellationTokenSource;
        readonly IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher;
        readonly MessagePipeInterprocessOptions options;
        public MessagePipeInterprocessOptions Options => options;

        // Channel is used from publisher for thread safety of write packet
        int initializedServer = 0;
        Lazy<SocketUdpServer> server;
        Channel<UdpMessageContainer> channel;

        int initializedClient = 0;
        Lazy<SocketUdpClient> client;

        // サーバーとクライアントの有効性を追跡するプロパティ
        private bool IsServerValid => server.IsValueCreated && server.Value.IsValid;
        private bool IsClientValid => client.IsValueCreated; // クライアントは常に作成可能と仮定

        // create from DI
        [Preserve]
        public UdpWorker(MessagePipeInterprocessUdpOptions options, IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher)
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Initializing with options - Host: " + options.Host + ", Port: " + options.Port 
                + ", IgnoreBindErrors: " + options.IgnoreBindErrors 
                + ", IgnoreSendErrors: " + options.IgnoreSendErrors);
            #endif
            
            this.cancellationTokenSource = new CancellationTokenSource();
            this.options = options;
            this.publisher = publisher;

            // UDPサーバーの初期化時にIgnoreBindErrorsオプションを渡す
            this.server = new Lazy<SocketUdpServer>(() => 
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Creating UDP server on port " + options.Port);
                #endif
                
                return SocketUdpServer.Bind(options.Port, 0x10000, options.IgnoreBindErrors);
            });

            // UDPクライアントは、オプションにサブネットマスクとネットワークアドレスが指定されているかを反映
            this.client = new Lazy<SocketUdpClient>(() => 
            {
                // キャストしてオプションの拡張プロパティにアクセスする
                var udpOptions = options as MessagePipeInterprocessUdpOptions;
                
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Creating UDP client to " + udpOptions.Host + ":" + udpOptions.Port 
                    + (string.IsNullOrEmpty(udpOptions.SubnetMask) ? "" : ", SubnetMask: " + udpOptions.SubnetMask)
                    + (string.IsNullOrEmpty(udpOptions.NetworkAddress) ? "" : ", NetworkAddress: " + udpOptions.NetworkAddress));
                #endif
                
                return SocketUdpClient.Connect(udpOptions.Host, udpOptions.Port, 0x10000, udpOptions.SubnetMask, udpOptions.NetworkAddress);
            });

    #if !UNITY_2018_3_OR_NEWER
            this.channel = Channel.CreateUnbounded<UdpMessageContainer>(new UnboundedChannelOptions()
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true
            });
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Created unbounded channel for messages");
            #endif
    #else
            this.channel = Channel.CreateSingleConsumerUnbounded<UdpMessageContainer>();
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Created single consumer unbounded channel for messages");
            #endif
    #endif
        }

    #if NET5_0_OR_GREATER
        [Preserve]
        public UdpWorker(MessagePipeInterprocessUdpUdsOptions options, IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher)
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Initializing with UDS options - SocketPath: " + options.SocketPath 
                + ", IgnoreBindErrors: " + options.IgnoreBindErrors 
                + ", IgnoreSendErrors: " + options.IgnoreSendErrors);
            #endif
            
            this.cancellationTokenSource = new CancellationTokenSource();
            this.options = options;
            this.publisher = publisher;

            // UDPサーバーの初期化時にIgnoreBindErrorsオプションを渡す
            this.server = new Lazy<SocketUdpServer>(() => 
            {
                // UDSオプションにもIgnoreBindErrorsプロパティが必要
                var ignoreBindErrors = options is MessagePipeInterprocessUdpUdsOptions udpUdsOptions ? udpUdsOptions.IgnoreBindErrors : false;
                
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Creating UDP server with UDS socket path: " + options.SocketPath);
                #endif
                
                return SocketUdpServer.BindUds(options.SocketPath, 0x10000, ignoreBindErrors);
            });

            this.client = new Lazy<SocketUdpClient>(() => 
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Creating UDP client with UDS socket path: " + options.SocketPath);
                #endif
                
                return SocketUdpClient.ConnectUds(options.SocketPath, 0x10000);
            });

    #if !UNITY_2018_3_OR_NEWER
            this.channel = Channel.CreateUnbounded<UdpMessageContainer>(new UnboundedChannelOptions()
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true
            });
    #else
            this.channel = Channel.CreateSingleConsumerUnbounded<UdpMessageContainer>();
    #endif
        }
    #endif

        /// <summary>
        /// デフォルトの送信先アドレスを使用してメッセージを発行します
        /// </summary>
        public void Publish<TKey, TMessage>(TKey key, TMessage message)
        {
            Publish(key, message, null);
        }

        /// <summary>
        /// 指定された送信先アドレスを使用してメッセージを発行します
        /// null の場合はデフォルトの送信先が使用されます
        /// </summary>
        public void Publish<TKey, TMessage>(TKey key, TMessage message, string toAddress)
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Publishing message of type " + typeof(TMessage).Name + " with key type " + typeof(TKey).Name 
                + (string.IsNullOrEmpty(toAddress) ? " to default address" : " to address: " + toAddress));
            #endif
            
            if (Interlocked.Increment(ref initializedClient) == 1) // first incr, channel not yet started
            {
                try
                {
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] First publish - initializing client");
                    #endif
                    
                    _ = client.Value; // init
                    RunPublishLoop();
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] Client initialized and publish loop started");
                    #endif
                }
                catch (Exception ex)
                {
                    // クライアント初期化に失敗した場合
                    Interlocked.Exchange(ref initializedClient, 0); // リセット
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.LogError("[UdpWorker] Failed to initialize client: " + ex.Message);
                    #endif
                    
                    // IgnoreSendErrorsが有効な場合は例外を無視
                    if (options is MessagePipeInterprocessUdpOptions udpOptions && udpOptions.IgnoreSendErrors)
                    {
                        options.UnhandledErrorHandler("UDP client initialization failed, but continuing due to IgnoreSendErrors option.", ex);
                        
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.LogWarning("[UdpWorker] Ignoring client initialization error due to IgnoreSendErrors option");
                        #endif
                        
                        return;
                    }
                    
                    // それ以外は例外を再スロー
                    throw;
                }
            }

            // クライアントが無効な場合は何もしない
            if (!IsClientValid)
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.LogWarning("[UdpWorker] Client is not valid, message will not be sent");
                #endif
                
                return;
            }

            var buffer = MessageBuilder.BuildPubSubMessage(key, message, options.MessagePackSerializerOptions);
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Built message buffer with size: " + buffer.Length + " bytes");
            #endif
            
            channel.Writer.TryWrite(new UdpMessageContainer { Data = buffer, ToAddress = toAddress });
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Message queued for sending");
            #endif
        }

        /// <summary>
        /// 送信先アドレスとポートを指定してメッセージを発行します
        /// </summary>
        public void PublishToTarget<TKey, TMessage>(TKey key, TMessage message, string targetAddress, int targetPort)
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Publishing message of type " + typeof(TMessage).Name + " with key type " + typeof(TKey).Name 
                + " to specific target: " + targetAddress + ":" + targetPort);
            #endif
            
            if (Interlocked.Increment(ref initializedClient) == 1) // first incr, channel not yet started
            {
                try
                {
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] First publish - initializing client for targeted message");
                    #endif
                    
                    _ = client.Value; // init
                    RunPublishLoop();
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] Client initialized and publish loop started for targeted message");
                    #endif
                }
                catch (Exception ex)
                {
                    // クライアント初期化に失敗した場合
                    Interlocked.Exchange(ref initializedClient, 0); // リセット
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.LogError("[UdpWorker] Failed to initialize client for targeted message: " + ex.Message);
                    #endif
            
                    // IgnoreSendErrorsが有効な場合は例外を無視
                    if (options is MessagePipeInterprocessUdpOptions udpOptions && udpOptions.IgnoreSendErrors)
                    {
                        options.UnhandledErrorHandler("UDP client initialization failed, but continuing due to IgnoreSendErrors option.", ex);
                        
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.LogWarning("[UdpWorker] Ignoring client initialization error due to IgnoreSendErrors option");
                        #endif
                        
                        return;
                    }
            
                    // それ以外は例外を再スロー
                    throw;
                }
            }

            // クライアントが無効な場合は何もしない
            if (!IsClientValid)
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.LogWarning("[UdpWorker] Client is not valid, targeted message will not be sent");
                #endif
                
                return;
            }

            var buffer = MessageBuilder.BuildPubSubMessage(key, message, options.MessagePackSerializerOptions);
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Built targeted message buffer with size: " + buffer.Length + " bytes, target: " 
                + targetAddress + ":" + targetPort);
            #endif
            
            channel.Writer.TryWrite(new UdpMessageContainer { 
                Data = buffer, 
                ToAddress = targetAddress,
                Port = targetPort
            });
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Targeted message queued for sending");
            #endif
        }

        // Send packet to udp socket from publisher
        async void RunPublishLoop()
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Starting publish loop");
            #endif
            
            var reader = channel.Reader;
            var token = cancellationTokenSource.Token;
            var udpClient = client.Value;
            
            // クライアントが無効な場合はループを終了
            if (!IsClientValid)
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.LogError("[UdpWorker] Client is not valid, publish loop will not start");
                #endif
                
                return;
            }
            
            int messageCount = 0;
            int errorCount = 0;
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Publish loop started successfully");
            #endif
            
            while (await reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    try
                    {
                        #if MESSAGEPIPE_UDP_DEBUG
                        var sendStartTime = DateTime.UtcNow;
                        var sendType = !string.IsNullOrEmpty(item.ToAddress) ? 
                            (item.Port.HasValue ? "targeted with specific port" : "targeted") : "default";
                        UnityEngine.Debug.Log("[UdpWorker] Sending " + sendType + " message, buffer size: " + item.Data.Length + " bytes");
                        #endif
                        
                        // 送信先アドレスとポートが指定されている場合
                        if (!string.IsNullOrEmpty(item.ToAddress) && item.Port.HasValue)
                        {
                            await udpClient.SendToAsync(item.Data, item.ToAddress, item.Port.Value, token).ConfigureAwait(false);
                            
                            #if MESSAGEPIPE_UDP_DEBUG
                            UnityEngine.Debug.Log("[UdpWorker] Sent message to specific target: " + item.ToAddress + ":" + item.Port.Value 
                                + ", took: " + (DateTime.UtcNow - sendStartTime).TotalMilliseconds + "ms");
                            #endif
                        }
                        // 送信先アドレスのみ指定されている場合
                        else if (!string.IsNullOrEmpty(item.ToAddress))
                        {
                            await udpClient.SendToAsync(item.Data, item.ToAddress, token).ConfigureAwait(false);
                            
                            #if MESSAGEPIPE_UDP_DEBUG
                            UnityEngine.Debug.Log("[UdpWorker] Sent message to address: " + item.ToAddress 
                                + ", took: " + (DateTime.UtcNow - sendStartTime).TotalMilliseconds + "ms");
                            #endif
                        }
                        // デフォルト送信先を使用
                        else
                        {
                            await udpClient.SendAsync(item.Data, token).ConfigureAwait(false);
                            
                            #if MESSAGEPIPE_UDP_DEBUG
                            UnityEngine.Debug.Log("[UdpWorker] Sent message to default endpoint, took: " 
                                + (DateTime.UtcNow - sendStartTime).TotalMilliseconds + "ms");
                            #endif
                        }
                        
                        messageCount++;
                        
                        #if MESSAGEPIPE_UDP_DEBUG
                        if (messageCount % 100 == 0)
                        {
                            UnityEngine.Debug.Log("[UdpWorker] Publish statistics - Messages sent: " + messageCount + ", Errors: " + errorCount);
                        }
                        #endif
                    }
                    catch (Exception ex)
                    {
                        // 例外処理（変更なし）
                        if (ex is OperationCanceledException || token.IsCancellationRequested)
                        {
                            #if MESSAGEPIPE_UDP_DEBUG
                            UnityEngine.Debug.Log("[UdpWorker] Publish loop canceled");
                            #endif
                            return;
                        }

                        errorCount++;
                        
                        // ソケットエラーの場合
                        if (ex is SocketException socketEx)
                        {
                            #if MESSAGEPIPE_UDP_DEBUG
                            UnityEngine.Debug.LogError("[UdpWorker] Socket error during send: " + socketEx.SocketErrorCode + ", ErrorCode: " + socketEx.ErrorCode);
                            #endif
                            
                            // IgnoreSendErrors が true の場合は継続
                            if (options is MessagePipeInterprocessUdpOptions udpOptions && udpOptions.IgnoreSendErrors)
                            {
                                options.UnhandledErrorHandler("Network send error, continuing due to IgnoreSendErrors option.", ex);
                                
                                #if MESSAGEPIPE_UDP_DEBUG
                                UnityEngine.Debug.LogWarning("[UdpWorker] Ignoring send error and continuing due to IgnoreSendErrors option");
                                #endif
                                
                                continue; // 次のメッセージへ
                            }
                        }

                        // その他のエラーまたはIgnoreSendErrorsがfalseの場合は従来通り処理
                        options.UnhandledErrorHandler("network error, publish loop will terminate." + Environment.NewLine, ex);
                        
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.LogError("[UdpWorker] Fatal error in publish loop, terminating: " + ex.Message);
                        #endif
                        
                        return;
                    }
                }
            }
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Publish loop completed normally");
            #endif
        }

        public void StartReceiver()
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Starting receiver");
            #endif
            
            if (Interlocked.Increment(ref initializedServer) == 1) // first incr, channel not yet started
            {
                try
                {
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] First receiver start - initializing server");
                    #endif
                    
                    _ = server.Value; // init
                    
                    // サーバーが有効な場合のみ受信ループを開始
                    if (IsServerValid)
                    {
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.Log("[UdpWorker] Server valid, starting receive loop");
                        #endif
                        
                        RunReceiveLoop();
                    }
                    else
                    {
                        // サーバーが無効な場合はログに記録
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.LogWarning("[UdpWorker] Server is in invalid state. Receiver will not start.");
                        #endif
                        
                        options.UnhandledErrorHandler("UDP server is in invalid state. Receiver will not start.", null);
                    }
                }
                catch (Exception ex)
                {
                    // サーバー初期化に失敗した場合
                    Interlocked.Exchange(ref initializedServer, 0); // リセット
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.LogError("[UdpWorker] Failed to initialize server: " + ex.Message);
                    #endif
                    
                    throw; // 例外を再スロー
                }
            }
            else
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Receiver already started");
                #endif
            }
        }

        // Receive from udp socket and push value to subscribers.
        async void RunReceiveLoop()
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Starting receive loop");
            #endif
            
            var token = cancellationTokenSource.Token;
            var udpServer = server.Value;
            
            // サーバーが無効な場合はループを開始しない
            if (!IsServerValid)
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.LogError("[UdpWorker] Server is not valid, receive loop will not start");
                #endif
                
                return;
            }
            
            int messageCount = 0;
            int errorCount = 0;
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Receive loop started successfully");
            #endif
            
            while (!token.IsCancellationRequested)
            {
                ReadOnlyMemory<byte> value;
                try
                {
                    #if MESSAGEPIPE_UDP_DEBUG
                    var receiveStartTime = DateTime.UtcNow;
                    #endif
                    
                    value = await udpServer.ReceiveAsync(token).ConfigureAwait(false);
                    
                    if (value.Length == 0)
                    {
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.Log("[UdpWorker] Received empty packet, ignoring");
                        #endif
                        
                        continue; // ゼロ長パケットは無視
                    }
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] Received packet with size: " + value.Length + " bytes, took: " 
                        + (DateTime.UtcNow - receiveStartTime).TotalMilliseconds + "ms");
                    #endif
                    
                    var len = MessageBuilder.FetchMessageLength(value.Span);
                    if (len != value.Length - 4)
                    {
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.LogError("[UdpWorker] Invalid message size. Expected: " + len + ", Actual: " + (value.Length - 4));
                        #endif
                        
                        throw new InvalidOperationException("Receive invalid message size.");
                    }
                    value = value.Slice(4);
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] Message extracted, payload size: " + value.Length + " bytes");
                    #endif
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException || token.IsCancellationRequested)
                    {
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.Log("[UdpWorker] Receive loop canceled");
                        #endif
                        
                        return;
                    }
                    
                    errorCount++;
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.LogError("[UdpWorker] Error during packet receive: " + ex.Message);
                    #endif
                    
                    options.UnhandledErrorHandler("network error, receive loop will terminate." + Environment.NewLine, ex);
                    return;
                }

                try
                {
                    #if MESSAGEPIPE_UDP_DEBUG
                    var processStartTime = DateTime.UtcNow;
                    #endif
                    
                    var message = MessageBuilder.ReadPubSubMessage(value.ToArray());
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] Message parsed with type: " + message.MessageType + ", key length: " 
                        + message.KeyMemory.Length + ", value length: " + message.ValueMemory.Length);
                    #endif
                    
                    publisher.Publish(message, message, CancellationToken.None);
                    
                    messageCount++;
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.Log("[UdpWorker] Message published to subscribers, took: " 
                        + (DateTime.UtcNow - processStartTime).TotalMilliseconds + "ms");
                        
                    if (messageCount % 100 == 0)
                    {
                        UnityEngine.Debug.Log("[UdpWorker] Receive statistics - Messages received: " + messageCount + ", Errors: " + errorCount);
                    }
                    #endif
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException || token.IsCancellationRequested)
                    {
                        #if MESSAGEPIPE_UDP_DEBUG
                        UnityEngine.Debug.Log("[UdpWorker] Message processing canceled");
                        #endif
                        
                        return;
                    }
                    
                    errorCount++;
                    
                    #if MESSAGEPIPE_UDP_DEBUG
                    UnityEngine.Debug.LogError("[UdpWorker] Error processing message: " + ex.Message);
                    #endif
                    
                    options.UnhandledErrorHandler("", ex);
                }
            }
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Receive loop completed normally");
            #endif
        }

        public void Dispose()
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Disposing resources");
            #endif
            
            channel.Writer.TryComplete();
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Channel writer completed");
            #endif

            cancellationTokenSource.Cancel();
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Cancellation requested");
            #endif
            
            cancellationTokenSource.Dispose();
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] CancellationTokenSource disposed");
            #endif

            if (server.IsValueCreated)
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Disposing server");
                #endif
                
                server.Value.Dispose();
                
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Server disposed");
                #endif
            }

            if (client.IsValueCreated)
            {
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Disposing client");
                #endif
                
                client.Value.Dispose();
                
                #if MESSAGEPIPE_UDP_DEBUG
                UnityEngine.Debug.Log("[UdpWorker] Client disposed");
                #endif
            }
            
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpWorker] Dispose completed");
            #endif
        }
    }
}
