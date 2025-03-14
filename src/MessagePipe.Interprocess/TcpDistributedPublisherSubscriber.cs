using MessagePipe.Interprocess.Extended;
using MessagePipe.Interprocess.Internal;
using MessagePipe.Interprocess.Workers;
using System;
using System.Threading;
using Cysharp.Threading.Tasks;

namespace MessagePipe.Interprocess
{
    [Preserve]
    public sealed class TcpDistributedPublisher<TKey, TMessage> : IDistributedPublisher<TKey, TMessage>
    {
        readonly TcpWorker worker;
        readonly MessagePipeInterprocessOptions options;

        [Preserve]
        public TcpDistributedPublisher(TcpWorker worker, MessagePipeInterprocessOptions options)
        {
            this.worker = worker;
            this.options = options;
        }

        public UniTask PublishAsync(TKey key, TMessage message, CancellationToken cancellationToken = default)
        {
            try
            {
                // メッセージがIToAddressableを実装しているかチェック
                if (message is IToAddressable addressable)
                {
                    // GetToAddress()を呼び出してアドレスを取得
                    string toAddress = addressable.GetToAddress();
                    if (!string.IsNullOrEmpty(toAddress))
                    {
                        // アドレスが取得できた場合
                        int? port = null;
                        
                        // メッセージがIToPortableも実装している場合はポートも取得
                        if (message is IToPortable portable)
                        {
                            int toPort = portable.GetPort();
                            if (toPort > 0)
                            {
                                port = toPort;
                            }
                        }
                        
                        // 送信先を指定するオーバーロードを呼び出す
                        return PublishToTargetAsync(key, message, toAddress, port, cancellationToken);
                    }
                }
                
                // IToAddressableを実装していない、またはアドレスが取得できなかった場合は従来の処理
                worker.Publish(key, message);
            }
            catch (Exception ex)
            {
                // 拡張オプションの場合はエラー無視設定を確認
                bool ignoreErrors = options is MessagePipeInterprocessTcpExtendedOptions extOptions && extOptions.IgnoreSendErrors;
                if (ignoreErrors)
                {
                    options.UnhandledErrorHandler?.Invoke("TCP send error, but continuing due to IgnoreSendErrors option.", ex);
                }
                else
                {
                    // それ以外は例外を再スロー
                    throw;
                }
            }
            return default;
        }

        // 送信先を指定するオーバーロードメソッド
        public UniTask PublishToTargetAsync(TKey key, TMessage message, string toAddress, int? port = null, CancellationToken cancellationToken = default)
        {
            try
            {
                worker.PublishToTarget(key, message, toAddress, port);
            }
            catch (Exception ex)
            {
                // 拡張オプションの場合はエラー無視設定を確認
                bool ignoreErrors = options is MessagePipeInterprocessTcpExtendedOptions extOptions && extOptions.IgnoreSendErrors;
                if (ignoreErrors)
                {
                    options.UnhandledErrorHandler?.Invoke($"TCP send error to {toAddress}:{port}, but continuing due to IgnoreSendErrors option.", ex);
                }
                else
                {
                    // それ以外は例外を再スロー
                    throw;
                }
            }
            return default;
        }
        
        // Fluent APIを使用するためのファクトリーメソッド
        public FluentTcpPublisher CreatePublisher()
        {
            return worker.CreatePublisher();
        }
    }

    [Preserve]
    public sealed class TcpDistributedSubscriber<TKey, TMessage> : IDistributedSubscriber<TKey, TMessage>
    {
        // Published from TcpWorker.
        readonly MessagePipeInterprocessOptions options;
        readonly IAsyncSubscriber<IInterprocessKey, IInterprocessValue> subscriberCore;
        readonly FilterAttachedMessageHandlerFactory syncHandlerFactory;
        readonly FilterAttachedAsyncMessageHandlerFactory asyncHandlerFactory;

        [Preserve]
        public TcpDistributedSubscriber(TcpWorker worker, MessagePipeInterprocessTcpOptions options, IAsyncSubscriber<IInterprocessKey, IInterprocessValue> subscriberCore, FilterAttachedMessageHandlerFactory syncHandlerFactory, FilterAttachedAsyncMessageHandlerFactory asyncHandlerFactory)
        {
            this.options = options;
            this.subscriberCore = subscriberCore;
            this.syncHandlerFactory = syncHandlerFactory;
            this.asyncHandlerFactory = asyncHandlerFactory;

            try
            {
                worker.StartReceiver();
            }
            catch (Exception ex)
            {
                // 拡張オプションの場合はエラー無視設定を確認
                bool ignoreErrors = options is MessagePipeInterprocessTcpExtendedOptions extOptions && extOptions.IgnoreConnectErrors;
                if (ignoreErrors)
                {
                    options.UnhandledErrorHandler?.Invoke("Failed to start TCP receiver, but continuing due to IgnoreConnectErrors option.", ex);
                }
                else
                {
                    // それ以外は例外を再スロー
                    throw;
                }
            }
        }

#if NET5_0_OR_GREATER
        [Preserve]
        public TcpDistributedSubscriber(TcpWorker worker, MessagePipeInterprocessTcpUdsOptions options, IAsyncSubscriber<IInterprocessKey, IInterprocessValue> subscriberCore, FilterAttachedMessageHandlerFactory syncHandlerFactory, FilterAttachedAsyncMessageHandlerFactory asyncHandlerFactory)
        {
            this.options = options;
            this.subscriberCore = subscriberCore;
            this.syncHandlerFactory = syncHandlerFactory;
            this.asyncHandlerFactory = asyncHandlerFactory;

            try
            {
                worker.StartReceiver();
            }
            catch (Exception ex)
            {
                // 拡張オプションの場合はエラー無視設定を確認
                bool ignoreErrors = options is MessagePipeInterprocessTcpUdsExtendedOptions extOptions && extOptions.IgnoreConnectErrors;
                if (ignoreErrors)
                {
                    options.UnhandledErrorHandler?.Invoke("Failed to start TCP receiver, but continuing due to IgnoreConnectErrors option.", ex);
                }
                else
                {
                    // それ以外は例外を再スロー
                    throw;
                }
            }
        }
#endif

        public UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IMessageHandler<TMessage> handler, CancellationToken cancellationToken = default)
        {
            return SubscribeAsync(key, handler, Array.Empty<MessageHandlerFilter<TMessage>>(), cancellationToken);
        }

        public UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IMessageHandler<TMessage> handler, MessageHandlerFilter<TMessage>[] filters, CancellationToken cancellationToken = default)
        {
            handler = syncHandlerFactory.CreateMessageHandler(handler, filters);
            var transform = new TransformSyncMessageHandler<TMessage>(handler, options.MessagePackSerializerOptions);
            return SubscribeCore(key, transform);
        }

        public UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IAsyncMessageHandler<TMessage> handler, CancellationToken cancellationToken = default)
        {
            return SubscribeAsync(key, handler, Array.Empty<AsyncMessageHandlerFilter<TMessage>>(), cancellationToken);
        }

        public UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IAsyncMessageHandler<TMessage> handler, AsyncMessageHandlerFilter<TMessage>[] filters, CancellationToken cancellationToken = default)
        {
            handler = asyncHandlerFactory.CreateAsyncMessageHandler(handler, filters);
            var transform = new TransformAsyncMessageHandler<TMessage>(handler, options.MessagePackSerializerOptions);
            return SubscribeCore(key, transform);
        }

        UniTask<IUniTaskAsyncDisposable> SubscribeCore(TKey key, IAsyncMessageHandler<IInterprocessValue> handler)
        {
            var byteKey = MessageBuilder.CreateKey(key, options.MessagePackSerializerOptions);
            var d = subscriberCore.Subscribe(byteKey, handler);
            return new UniTask<IUniTaskAsyncDisposable>(new AsyncDisposableBridge(d));
        }
    }
}
