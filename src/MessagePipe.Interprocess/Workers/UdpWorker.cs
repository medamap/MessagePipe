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
    [Preserve]
    public sealed class UdpWorker : IDisposable
    {
        readonly CancellationTokenSource cancellationTokenSource;
        readonly IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher;
        readonly MessagePipeInterprocessOptions options;

        // チャネルは送信パケットのスレッドセーフなキュー用
        int initializedServer = 0;
        Lazy<SocketUdpServer> server;
        Channel<byte[]> channel;

        int initializedClient = 0;
        Lazy<SocketUdpClient> client;

        // DI で作成
        [Preserve]
        public UdpWorker(MessagePipeInterprocessUdpOptions options, IAsyncPublisher<IInterprocessKey, IInterprocessValue> publisher)
        {
            this.cancellationTokenSource = new CancellationTokenSource();
            this.options = options;
            this.publisher = publisher;

            this.server = new Lazy<SocketUdpServer>(() =>
            {
                return SocketUdpServer.Bind(options.Port, 0x10000);
            });

            this.client = new Lazy<SocketUdpClient>(() =>
            {
                return SocketUdpClient.Connect(options.Host, options.Port, 0x10000);
            });

#if !UNITY_2018_3_OR_NEWER
            this.channel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions()
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true
            });
#else
            this.channel = Channel.CreateSingleConsumerUnbounded<byte[]>();
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
            this.channel = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions()
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = true
            });
#else
            this.channel = Channel.CreateSingleConsumerUnbounded<byte[]>();
#endif
        }
#endif

        public void Publish<TKey, TMessage>(TKey key, TMessage message)
        {
            if (Interlocked.Increment(ref initializedClient) == 1) // 最初の送信なら初期化＆ループ開始
            {
                _ = client.Value; // 初期化
                RunPublishLoop();
            }

            var buffer = MessageBuilder.BuildPubSubMessage(key, message, options.MessagePackSerializerOptions);
            channel.Writer.TryWrite(buffer);
        }

        // 送信ループ
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
                        await udpClient.SendAsync(item, token).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        if (ex is OperationCanceledException || token.IsCancellationRequested)
                            return;
                        // ログ出力し、短い待機後にループを継続（再試行）
                        options.UnhandledErrorHandler("Publish loop encountered network error; retrying after delay." + Environment.NewLine, ex);
                        await UniTask.Delay(TimeSpan.FromSeconds(1), cancellationToken: token);
                        // ※必要に応じて、クライアントの再初期化処理も検討する
                        continue;
                    }
                }
            }
        }

        public void StartReceiver()
        {
            if (Interlocked.Increment(ref initializedServer) == 1) // 初回なら初期化＆受信ループ開始
            {
                _ = server.Value; // 初期化
                RunReceiveLoop();
            }
        }

        // 受信ループ
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
                    if (value.Length == 0)
                    {
                        // ゼロ長パケットの場合は、ループを継続
                        continue;
                    }
                    var len = MessageBuilder.FetchMessageLength(value.Span);
                    if (len != value.Length - 4)
                    {
                        throw new InvalidOperationException("Received invalid message size.");
                    }
                    value = value.Slice(4);
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException || token.IsCancellationRequested)
                        break;
                    // ログ出力し、短い待機後に受信ループを再試行
                    options.UnhandledErrorHandler("Receive loop encountered network error; retrying after delay." + Environment.NewLine, ex);
                    await UniTask.Delay(TimeSpan.FromSeconds(1), cancellationToken: token);
                    continue;
                }

                try
                {
                    var message = MessageBuilder.ReadPubSubMessage(value.ToArray());
                    publisher.Publish(message, message, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    if (ex is OperationCanceledException || token.IsCancellationRequested)
                        break;
                    options.UnhandledErrorHandler("Error processing received message." + Environment.NewLine, ex);
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
