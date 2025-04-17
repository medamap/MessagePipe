using System;
using System.Threading;
using System.Threading.Tasks;
using Cysharp.Threading.Tasks;

namespace MessagePipe
{
    public interface IDistributedPublisher<in TKey, in TMessage>
    {
        UniTask PublishAsync(TKey key, TMessage message, CancellationToken cancellationToken = default);
    }

    public interface IDistributedSubscriber<in TKey, TMessage>
    {
        UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IMessageHandler<TMessage> handler, CancellationToken cancellationToken = default);
        UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IMessageHandler<TMessage> handler, MessageHandlerFilter<TMessage>[] filters, CancellationToken cancellationToken = default);
        UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IAsyncMessageHandler<TMessage> handler, CancellationToken cancellationToken = default);
        UniTask<IUniTaskAsyncDisposable> SubscribeAsync(TKey key, IAsyncMessageHandler<TMessage> handler, AsyncMessageHandlerFilter<TMessage>[] filters, CancellationToken cancellationToken = default);
    }
}