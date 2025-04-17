using MessagePipe.Interprocess.Internal;
using MessagePipe.Interprocess.Workers;
using System;
using System.Threading;
using System.Net.Sockets;
using Cysharp.Threading.Tasks;

namespace MessagePipe.Interprocess
{
    // 注意: IToAdressable インターフェースは削除せず、IToAddressable と同じ定義にする
    // これにより後方互換性を保ちつつ、新しいコードでは IToAddressable を使用できる
    public interface IToAdressable
    {
        string GetToAddress();
    }
    
    /// <summary>
    /// 破棄可能な発行者を表すインターフェース
    /// </summary>
    public interface IDisposablePublisher<T> : IDisposable
    {
        // 特別なメソッドは必要なく、IDisposableの継承だけで十分
    }
    
    [Preserve]
    public sealed class UdpDistributedPublisher<TKey, TMessage> : 
        IDistributedPublisher<TKey, TMessage>,
        IDisposablePublisher<TMessage>
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
                // 両方のインターフェースをチェック（後方互換性のため）
                if (message is IToAddressable addressable)
                {
                    string toAddress = addressable.GetToAddress();
                    if (!string.IsNullOrEmpty(toAddress))
                    {
                        // ポート指定もサポート
                        int? port = null;
                        if (message is IToPortable portable)
                        {
                            int toPort = portable.GetPort();
                            if (toPort > 0)
                            {
                                port = toPort;
                            }
                        }
                        
                        // 新しいオーバーロードを呼び出す
                        return PublishToTargetAsync(key, message, toAddress, port, cancellationToken);
                    }
                }
                else if (message is IToAdressable oldAddressable) // 古いインターフェースもサポート
                {
                    string toAddress = oldAddressable.GetToAddress();
                    if (!string.IsNullOrEmpty(toAddress))
                    {
                        // 古いインターフェースの場合はポート指定なし
                        return PublishToTargetAsync(key, message, toAddress, null, cancellationToken);
                    }
                }
        
                // 従来の処理
                worker.Publish(key, message, null); // デフォルトアドレス使用
            }
            catch (Exception ex)
            {
                // エラーを再スロー
                throw;
            }
            return default;
        }

        // 送信先アドレスとポートを指定するオーバーロード
        public UniTask PublishToTargetAsync(TKey key, TMessage message, string toAddress, int? port = null, CancellationToken cancellationToken = default)
        {
            try
            {
                // ポート指定がある場合は新しいメソッドを使用
                if (port.HasValue)
                {
                    worker.PublishToTarget(key, message, toAddress, port.Value);
                }
                else
                {
                    worker.Publish(key, message, toAddress);
                }
            }
            catch (Exception ex)
            {
                // エラーを再スロー
                throw;
            }
            return default;
        }
        
        // Fluent APIを使用するためのファクトリーメソッド
        public FluentUdpPublisher CreatePublisher()
        {
            return new FluentUdpPublisher(worker, worker.Options);
        }

        // IDisposableの実装
        public void Dispose()
        {
            #if MESSAGEPIPE_UDP_DEBUG
            UnityEngine.Debug.Log("[UdpDistributedPublisher] Disposing");
            #endif
            
            // UdpWorkerはDIコンテナによって管理されるため、
            // ここでは特に何もしない
            // 注: もし独自にworkerを生成したケースがあれば
            // その場合はDisposeが必要
        }
    }

    // UdpDistributedSubscriber は変更なし - 完全に保持
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
            #if MESSAGEPIPE_UDP_DEBUG
            global::UnityEngine.Debug.Log($"[UdpDistributedPublisherSubscriber] 1 SubscribeAsync({key} : {typeof(TKey).FullName}, handler, Array.Empty<MessageHandlerFilter<TMessage>>(), cancellationToken)");
            #endif
            return SubscribeAsync(key, handler, Array.Empty<MessageHandlerFilter<TMessage>>(), cancellationToken);
        }

        public UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IMessageHandler<TMessage> handler, MessageHandlerFilter<TMessage>[] filters, CancellationToken cancellationToken = default)
        {
            #if MESSAGEPIPE_UDP_DEBUG
            global::UnityEngine.Debug.Log($"[UdpDistributedPublisherSubscriber] 2 SubscribeCore({key} : {typeof(TKey).FullName}, transform)");
            #endif
            handler = syncHandlerFactory.CreateMessageHandler(handler, filters);
            var transform = new TransformSyncMessageHandler<TMessage>(handler, options.MessagePackSerializerOptions);
            return SubscribeCore(key, transform);
        }

        public UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IAsyncMessageHandler<TMessage> handler, CancellationToken cancellationToken = default)
        {
            #if MESSAGEPIPE_UDP_DEBUG
            global::UnityEngine.Debug.Log($"[UdpDistributedPublisherSubscriber] 3 SubscribeAsync({key} : {typeof(TKey).FullName}, handler, Array.Empty<AsyncMessageHandlerFilter<TMessage>>(), cancellationToken)");
            #endif
            return SubscribeAsync(key, handler, Array.Empty<AsyncMessageHandlerFilter<TMessage>>(), cancellationToken);
        }

        public UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IAsyncMessageHandler<TMessage> handler, AsyncMessageHandlerFilter<TMessage>[] filters, CancellationToken cancellationToken = default)
        {
            #if MESSAGEPIPE_UDP_DEBUG
            global::UnityEngine.Debug.Log($"[UdpDistributedPublisherSubscriber] 4 SubscribeCore({key} : {typeof(TKey).FullName}, transform)");
            #endif
            handler = asyncHandlerFactory.CreateAsyncMessageHandler(handler, filters);
            var transform = new TransformAsyncMessageHandler<TMessage>(handler, options.MessagePackSerializerOptions);
            return SubscribeCore(key, transform);
        }

        UniTask<IUniTaskAsyncDisposable> SubscribeCore(TKey key, IAsyncMessageHandler<IInterprocessValue> handler)
        {
            #if MESSAGEPIPE_UDP_DEBUG
            global::UnityEngine.Debug.Log($"[UdpDistributedPublisherSubscriber] 5 subscriberCore.Subscribe({key} : {typeof(TKey).FullName}, byteKey, handler)");
            #endif
            var byteKey = MessageBuilder.CreateKey(key, options.MessagePackSerializerOptions);
            var d = subscriberCore.Subscribe(byteKey, handler);
            return new UniTask<IUniTaskAsyncDisposable>(new AsyncDisposableBridge(d));
        }
    }
}
