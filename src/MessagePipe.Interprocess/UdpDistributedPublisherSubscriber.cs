using MessagePipe.Interprocess.Internal;
using MessagePipe.Interprocess.Workers;
using System;
using System.Threading;
using Cysharp.Threading.Tasks;

namespace MessagePipe.Interprocess
{
    [Preserve]
    public sealed class UdpDistributedPublisher<TKey, TMessage> : IDistributedPublisher<TKey, TMessage>
    {
        readonly UdpWorker worker;

        [Preserve]
        public UdpDistributedPublisher(UdpWorker worker)
        {
            this.worker = worker;
        }

        public UniTask PublishAsync(TKey key, TMessage message, CancellationToken cancellationToken = default)
        {
            try
            {
                worker.Publish(key, message, null); // Use default address
            }
            catch (Exception ex)
            {
                // IgnoreSendErrorsが有効な場合は例外を無視
                if (worker.Options is MessagePipeInterprocessUdpOptions udpOptions && udpOptions.IgnoreSendErrors)
                {
                    // 例外を無視して続行
                }
                else
                {
                    // それ以外は例外を再スロー
                    throw;
                }
            }
            return default;
        }

        // オーバーロードメソッドの追加: 送信先アドレスを動的に指定できるようにする
        public UniTask PublishAsync(TKey key, TMessage message, string toAddress, CancellationToken cancellationToken = default)
        {
            try
            {
                worker.Publish(key, message, toAddress); // Use specified address
            }
            catch (Exception ex)
            {
                // IgnoreSendErrorsが有効な場合は例外を無視
                if (worker.Options is MessagePipeInterprocessUdpOptions udpOptions && udpOptions.IgnoreSendErrors)
                {
                    // 例外を無視して続行
                }
                else
                {
                    // それ以外は例外を再スロー
                    throw;
                }
            }
            return default;
        }
    }

    [Preserve]
    public sealed class UdpDistributedSubscriber<TKey, TMessage> : IDistributedSubscriber<TKey, TMessage>
    {
        // Pubsished from UdpWorker.
        readonly MessagePipeInterprocessOptions options;
        readonly IAsyncSubscriber<IInterprocessKey, IInterprocessValue> subscriberCore;
        readonly FilterAttachedMessageHandlerFactory syncHandlerFactory;
        readonly FilterAttachedAsyncMessageHandlerFactory asyncHandlerFactory;

        [Preserve]
        public UdpDistributedSubscriber(UdpWorker worker, MessagePipeInterprocessUdpOptions options, IAsyncSubscriber<IInterprocessKey, IInterprocessValue> subscriberCore, FilterAttachedMessageHandlerFactory syncHandlerFactory, FilterAttachedAsyncMessageHandlerFactory asyncHandlerFactory)
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
                // IgnoreBindErrorsが有効な場合は例外を無視
                if (options.IgnoreBindErrors)
                {
                    options.UnhandledErrorHandler("Failed to start UDP receiver, but continuing due to IgnoreBindErrors option.", ex);
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
        public UdpDistributedSubscriber(UdpWorker worker, MessagePipeInterprocessUdpUdsOptions options, IAsyncSubscriber<IInterprocessKey, IInterprocessValue> subscriberCore, FilterAttachedMessageHandlerFactory syncHandlerFactory, FilterAttachedAsyncMessageHandlerFactory asyncHandlerFactory)
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
                // IgnoreBindErrorsが有効な場合は例外を無視
                if (options is MessagePipeInterprocessUdpUdsOptions udpUdsOptions && udpUdsOptions.IgnoreBindErrors)
                {
                    options.UnhandledErrorHandler("Failed to start UDP receiver, but continuing due to IgnoreBindErrors option.", ex);
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
