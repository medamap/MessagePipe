using MessagePack;
using System.Threading;
using Cysharp.Threading.Tasks;

namespace MessagePipe.Interprocess.Internal
{
    // TransformHandler.cs - TransformSyncMessageHandler クラスの修正
    [Preserve]
    internal sealed class TransformSyncMessageHandler<TMessage> : IAsyncMessageHandler<IInterprocessValue> 
    {
        readonly IMessageHandler<TMessage> handler;
        readonly MessagePackSerializerOptions options;

        [Preserve]
        public TransformSyncMessageHandler(IMessageHandler<TMessage> handler, MessagePackSerializerOptions options)
        {
            this.handler = handler;
            this.options = options;
        }

        [Preserve]
        public UniTask HandleAsync(IInterprocessValue message, CancellationToken cancellationToken)
        {
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[TransformSyncMessageHandler] HandleAsync called");
            UnityEngine.Debug.Log($"[TransformSyncMessageHandler] Message bytes length: {message.ValueMemory.Length}");
            #endif
            
            var msg = MessagePackSerializer.Deserialize<TMessage>(message.ValueMemory, options);
            
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[TransformSyncMessageHandler] Deserialized message: {msg}");
            #endif
            
            handler.Handle(msg);
            return default;
        }
    }

    // TransformHandler.cs - TransformAsyncMessageHandler クラスの修正
    [Preserve]
    internal sealed class TransformAsyncMessageHandler<TMessage> : IAsyncMessageHandler<IInterprocessValue> 
    {
        readonly IAsyncMessageHandler<TMessage> handler;
        readonly MessagePackSerializerOptions options;

        [Preserve]
        public TransformAsyncMessageHandler(IAsyncMessageHandler<TMessage> handler, MessagePackSerializerOptions options)
        {
            this.handler = handler;
            this.options = options;
        }

        [Preserve]
        public async UniTask HandleAsync(IInterprocessValue message, CancellationToken cancellationToken)
        {
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[TransformAsyncMessageHandler] HandleAsync called");
            UnityEngine.Debug.Log($"[TransformAsyncMessageHandler] Message bytes length: {message.ValueMemory.Length}");
            #endif
            
            var msg = MessagePackSerializer.Deserialize<TMessage>(message.ValueMemory, options);
            
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[TransformAsyncMessageHandler] Deserialized message: {msg}");
            #endif
            
            await handler.HandleAsync(msg, cancellationToken);
        }
    }

}
