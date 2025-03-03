using MessagePipe.Interprocess.Internal;
using System;
using System.Threading;
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
    }

    [Preserve]
    public sealed class UdpWorker : IDisposable
    {
        readonly CancellationTokenSource cancellationTokenSource;
        readonly IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher;
        readonly MessagePipeInterprocessOptions options;

        // Channel is used from publisher for thread safety of write packet
        int initializedServer = 0;
        Lazy<SocketUdpServer> server;
        Channel<UdpMessageContainer> channel;

        int initializedClient = 0;
        Lazy<SocketUdpClient> client;

        // create from DI
        [Preserve]
        public UdpWorker(MessagePipeInterprocessUdpOptions options, IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher)
        {
            this.cancellationTokenSource = new CancellationTokenSource();
            this.options = options;
            this.publisher = publisher;

            // UDPサーバーはそのままでOK
            this.server = new Lazy<SocketUdpServer>(() =>
            {
                return SocketUdpServer.Bind(options.Port, 0x10000);
            });

            // UDPクライアントは、オプションにサブネットマスクとネットワークアドレスが指定されているかを反映
            this.client = new Lazy<SocketUdpClient>(() =>
            {
                // キャストしてオプションの拡張プロパティにアクセスする
                var udpOptions = options as MessagePipeInterprocessUdpOptions;
                return SocketUdpClient.Connect(udpOptions.Host, udpOptions.Port, 0x10000, udpOptions.SubnetMask, udpOptions.NetworkAddress);
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

#if NET5_0_OR_GREATER
        [Preserve]
        public UdpWorker(MessagePipeInterprocessUdpUdsOptions options, IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher)
        {
            this.cancellationTokenSource = new CancellationTokenSource();
            this.options = options;
            this.publisher = publisher;

            this.server = new Lazy<SocketUdpServer>(() =>
            {
                return SocketUdpServer.BindUds(options.SocketPath, 0x10000);
            });

            this.client = new Lazy<SocketUdpClient>(() =>
            {
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
            if (Interlocked.Increment(ref initializedClient) == 1) // first incr, channel not yet started
            {
                _ = client.Value; // init
                RunPublishLoop();
            }

            var buffer = MessageBuilder.BuildPubSubMessage(key, message, options.MessagePackSerializerOptions);
            channel.Writer.TryWrite(new UdpMessageContainer { Data = buffer, ToAddress = toAddress });
        }

        // Send packet to udp socket from publisher
        async void RunPublishLoop()
        {
            var reader = channel.Reader;
            var token = cancellationTokenSource.Token;
            var udpClient = client.Value;
            while (await reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                while (reader.TryRead(out var item))
                {
                    try
                    {
                        // 送信先アドレスが指定されている場合は特定のアドレスに送信、そうでなければデフォルト送信先を使用
                        if (!string.IsNullOrEmpty(item.ToAddress))
                        {
                            await udpClient.SendToAsync(item.Data, item.ToAddress, token).ConfigureAwait(false);
                        }
                        else
                        {
                            await udpClient.SendAsync(item.Data, token).ConfigureAwait(false);
                        }
                    }
                    catch (Exception ex)
                    {
                        if (ex is OperationCanceledException || token.IsCancellationRequested)
                            return;

                        // network error, terminate.
                        options.UnhandledErrorHandler("network error, publish loop will terminate." + Environment.NewLine, ex);
                        return;
                    }
                }
            }
        }

        public void StartReceiver()
        {
            if (Interlocked.Increment(ref initializedServer) == 1) // first incr, channel not yet started
            {
                _ = server.Value; // init
                RunReceiveLoop();
            }
        }

        // Receive from udp socket and push value to subscribers.
        async void RunReceiveLoop()
        {
            var token = cancellationTokenSource.Token;
            var udpServer = server.Value;
            while (!token.IsCancellationRequested)
            {
                ReadOnlyMemory<byte> value;
                try
                {
                    value = await udpServer.ReceiveAsync(token).ConfigureAwait(false);
                    if (value.Length == 0) continue; // ゼロ長パケットは無視
                    var len = MessageBuilder.FetchMessageLength(value.Span);
                    if (len != value.Length - 4)
                    {
                        throw new InvalidOperationException("Receive invalid message size.");
                    }
                    value = value.Slice(4);
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException || token.IsCancellationRequested)
                        return;
                    options.UnhandledErrorHandler("network error, receive loop will terminate." + Environment.NewLine, ex);
                    return;
                }

                try
                {
                    var message = MessageBuilder.ReadPubSubMessage(value.ToArray());
                    publisher.Publish(message, message, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException || token.IsCancellationRequested)
                        return;
                    options.UnhandledErrorHandler("", ex);
                }
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
        }
    }
}