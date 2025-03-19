using MessagePack;
using MessagePack.Formatters;
using MessagePipe.Interprocess.Internal;
using System;
using System.Collections.Generic;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace MessagePipe.Interprocess
{
    public interface IInterprocessKey : IEquatable<IInterprocessKey>
    {
        ReadOnlyMemory<byte> KeyMemory { get; }
    }

    public interface IInterprocessValue
    {
        ReadOnlyMemory<byte> ValueMemory { get; }
    }

    // MessageBuilder.cs 内の InterprocessMessage クラスを改善（IL2CPP対応版）
    // MessageBuilder.cs 内のInterprocessMessageクラスの完全な再実装
    [Preserve]
    internal sealed class InterprocessMessage : IInterprocessKey, IInterprocessValue, IEquatable<InterprocessMessage>
    {
        readonly byte[] buffer;
        readonly int keyIndex;
        readonly int keyOffset;
        readonly int hashCode;

        public MessageType MessageType { get; }

        [Preserve]
        public InterprocessMessage(MessageType messageType, byte[] buffer, int keyIndex, int keyOffset)
        {
            this.MessageType = messageType;
            this.buffer = buffer;
            this.keyIndex = keyIndex;
            this.keyOffset = keyOffset;
            this.hashCode = CalcHashCode();
            
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            // 作成時にキー情報をデバッグ出力
            var keyBytes = new byte[keyOffset - keyIndex];
            Array.Copy(buffer, keyIndex, keyBytes, 0, keyBytes.Length);
            UnityEngine.Debug.Log($"[InterprocessMessage] Created with key bytes: {BitConverter.ToString(keyBytes)}");
            #endif
        }

        public ReadOnlyMemory<byte> KeyMemory => buffer.AsMemory(keyIndex, keyOffset - keyIndex);
        public ReadOnlyMemory<byte> ValueMemory => buffer.AsMemory(keyOffset, buffer.Length - keyOffset);

        // IInterprocessKey.Equals 実装の改善
        [Preserve]
        public bool Equals(IInterprocessKey other)
        {
            if (other == null) return false;
            
            var thisSpan = KeyMemory.Span;
            var otherSpan = other.KeyMemory.Span;
            
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[InterprocessMessage] Comparing keys - this: {BitConverter.ToString(thisSpan.ToArray())}, other: {BitConverter.ToString(otherSpan.ToArray())}");
            UnityEngine.Debug.Log($"[InterprocessMessage] Key lengths - this: {thisSpan.Length}, other: {otherSpan.Length}");
            #endif
            
            if (thisSpan.Length != otherSpan.Length) 
            {
                #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.Log($"[InterprocessMessage] Key length mismatch - returning false");
                #endif
                return false;
            }
            
            // バイトごとの厳密比較
            for (int i = 0; i < thisSpan.Length; i++)
            {
                if (thisSpan[i] != otherSpan[i]) 
                {
                    #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                    UnityEngine.Debug.Log($"[InterprocessMessage] Key byte mismatch at index {i} - this: {thisSpan[i]}, other: {otherSpan[i]} - returning false");
                    #endif
                    return false;
                }
            }
            
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[InterprocessMessage] Keys are equal - returning true");
            #endif
            return true;
        }
        
        // IEquatable<InterprocessMessage> 実装
        [Preserve]
        public bool Equals(InterprocessMessage other)
        {
            if (other == null) return false;
            if (ReferenceEquals(this, other)) return true;
            
            if (MessageType != other.MessageType) return false;
            
            return Equals(other as IInterprocessKey);
        }

        // Object.Equals のオーバーライド
        [Preserve]
        public override bool Equals(object obj)
        {
            if (obj is IInterprocessKey key)
                return Equals(key);
            return false;
        }

        [Preserve]
        public override int GetHashCode()
        {
            return hashCode;
        }

        [Preserve]
        int CalcHashCode()
        {
            // FNV1A32 ハッシュ（高性能で衝突が少ない）
            uint hash = 2166136261;
            for (int i = keyIndex; i < keyOffset; i++)
            {
                hash = unchecked((buffer[i] ^ hash) * 16777619);
            }
            return unchecked((int)hash);
        }
        
        // デバッグ用の文字列表現
        [Preserve]
        public override string ToString()
        {
            return $"InterprocessMessage[Type={MessageType}, KeyLength={KeyMemory.Length}, ValueLength={ValueMemory.Length}, Hash={GetHashCode()}]";
        }
    }

    internal enum MessageType : byte
    {
        PubSub = 1,
        RemoteRequest = 2,
        RemoteResponse = 3,
        RemoteError = 4,
    }

    internal static class MessageBuilder
    {
        // Message Frame-----
        // Length: int32(4), without self(MsgPack Body Only)
        // Body(PubSub): MessagePack Array[3](Type(byte), key, message)
        // Body(Reques): MessagePack Array[3](Type(byte), RequestHeader, request)
        // Body(Respon): MessagePack Array[3](Type(byte), messageId:int, response)
        // Body(RError): MessagePack Array[3](Type(byte), messageId:int, error:string)

// CreateKey メソッドの修正 - MessagePackSerializerOptions の適用方法を統一
        [Preserve]
        public static IInterprocessKey CreateKey<TKey>(TKey key, MessagePackSerializerOptions options)
        {
            // デバッグログを追加し、シリアライズ処理を明確化
            var bytes = MessagePackSerializer.Serialize(key, options);
    
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[MessageBuilder] CreateKey for: {key}, Type: {typeof(TKey).FullName}");
            UnityEngine.Debug.Log($"[MessageBuilder] Options resolver: {options.Resolver.GetType().Name}");
            UnityEngine.Debug.Log($"[MessageBuilder] Key bytes: {BitConverter.ToString(bytes)}");
            
            // キーの詳細情報（文字列の場合）
            if (key is string strKey)
            {
                UnityEngine.Debug.Log($"[MessageBuilder] String key: \"{strKey}\"");
            }
            
            // シリアライザーのオプション詳細
            UnityEngine.Debug.Log($"[MessageBuilder] Serializer options: Resolver={options.Resolver.GetType().Name}, Compression={options.Compression}");
#endif
    
            return new InterprocessMessage(MessageType.PubSub, bytes, 0, bytes.Length);
        }

// MessageBuilder.cs - BuildPubSubMessage メソッドの修正
        [Preserve]
        public static byte[] BuildPubSubMessage<TKey, TMessage>(TKey key, TMessage message, MessagePackSerializerOptions options)
        {
#if MESSAGEPIPE_TCP_SEND_DEBUG
            UnityEngine.Debug.Log($"[MessageBuilder] BuildPubSubMessage - Key: {key}, Message: {message}");
#endif
    
            using (var bufferWriter = new ArrayPoolBufferWriter())
            {
                var writer = new MessagePackWriter(bufferWriter);
                writer.WriteArrayHeader(3);
                writer.Write((byte)MessageType.PubSub);
        
#if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.Log($"[MessageBuilder] Serializing key of type: {typeof(TKey).FullName}");
#endif
        
                // キーのシリアライズを実行
                MessagePackSerializer.Serialize(ref writer, key, options);
        
#if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.Log($"[MessageBuilder] Serializing message of type: {typeof(TMessage).FullName}");
#endif
        
                // メッセージのシリアライズを実行
                MessagePackSerializer.Serialize(ref writer, message, options);
                writer.Flush();

                var finalBuffer = new byte[4 + bufferWriter.WrittenCount];
        
                // 長さヘッダーを追加（リトルエンディアン）
                Unsafe.WriteUnaligned(ref finalBuffer[0], bufferWriter.WrittenCount);
        
                bufferWriter.WrittenSpan.CopyTo(finalBuffer.AsSpan(4));
        
#if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.Log($"[MessageBuilder] Final buffer size: {finalBuffer.Length} bytes");
                
                // バッファの先頭を表示（デバッグ用）
                var previewLength = Math.Min(finalBuffer.Length, 32);
                UnityEngine.Debug.Log($"[MessageBuilder] Buffer preview: {BitConverter.ToString(finalBuffer, 0, previewLength)}");
#endif
        
                return finalBuffer;
            }
        }

        public static byte[] BuildRemoteRequestMessage<TRequest>(Type requestType, Type responseType, int messageId, TRequest message, MessagePackSerializerOptions options)
        {
            using (var bufferWriter = new ArrayPoolBufferWriter())
            {
                var writer = new MessagePackWriter(bufferWriter);
                writer.WriteArrayHeader(3);
                writer.Write((byte)MessageType.RemoteRequest);
                MessagePackSerializer.Serialize(ref writer, new RequestHeader(messageId, requestType.FullName, responseType.FullName), options);
                MessagePackSerializer.Serialize(ref writer, message, options);
                writer.Flush();

                var finalBuffer = new byte[4 + bufferWriter.WrittenCount];
                Unsafe.WriteUnaligned(ref finalBuffer[0], bufferWriter.WrittenCount);
                bufferWriter.WrittenSpan.CopyTo(finalBuffer.AsSpan(4));
                return finalBuffer;
            }
        }

        public static byte[] BuildRemoteResponseMessage(int messageId, Type responseType, object message, MessagePackSerializerOptions options)
        {
            using (var bufferWriter = new ArrayPoolBufferWriter())
            {
                var writer = new MessagePackWriter(bufferWriter);
                writer.WriteArrayHeader(3);
                writer.Write((byte)MessageType.RemoteResponse);
                MessagePackSerializer.Serialize(ref writer, messageId, options);
                MessagePackSerializer.Serialize(responseType, ref writer, message, options);
                writer.Flush();

                var finalBuffer = new byte[4 + bufferWriter.WrittenCount];
                Unsafe.WriteUnaligned(ref finalBuffer[0], bufferWriter.WrittenCount);
                bufferWriter.WrittenSpan.CopyTo(finalBuffer.AsSpan(4));
                return finalBuffer;
            }
        }

        public static byte[] BuildRemoteResponseError(int messageId, string exception, MessagePackSerializerOptions options)
        {
            using (var bufferWriter = new ArrayPoolBufferWriter())
            {
                var writer = new MessagePackWriter(bufferWriter);
                writer.WriteArrayHeader(3);
                writer.Write((byte)MessageType.RemoteError);
                MessagePackSerializer.Serialize(ref writer, messageId, options);
                MessagePackSerializer.Serialize(ref writer, exception, options);
                writer.Flush();

                var finalBuffer = new byte[4 + bufferWriter.WrittenCount];
                Unsafe.WriteUnaligned(ref finalBuffer[0], bufferWriter.WrittenCount);
                bufferWriter.WrittenSpan.CopyTo(finalBuffer.AsSpan(4));
                return finalBuffer;
            }
        }

        public static int FetchMessageLength(ReadOnlySpan<byte> xs)
        {
            return Unsafe.ReadUnaligned<int>(ref Unsafe.AsRef(xs[0]));
        }

        [Preserve]
        public static InterprocessMessage ReadPubSubMessage(byte[] buffer)
        {
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[MessageBuilder] ReadPubSubMessage - Buffer size: {buffer.Length} bytes");
            
            // バッファの先頭を表示（デバッグ用）
            var previewLength = Math.Min(buffer.Length, 32);
            UnityEngine.Debug.Log($"[MessageBuilder] Buffer preview: {BitConverter.ToString(buffer, 0, previewLength)}");
            #endif
            
            var reader = new MessagePackReader(buffer);
            
            var arrayLength = reader.ReadArrayHeader();
            if (arrayLength != 3)
            {
                #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.LogError($"[MessageBuilder] Invalid array length: {arrayLength}, expected 3");
                #endif
                
                throw new InvalidOperationException($"Invalid messagepack buffer. Expected array length 3, got {arrayLength}");
            }

            var msgTypeByte = reader.ReadByte();
            var msgType = (MessageType)msgTypeByte;
            
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[MessageBuilder] Message type: {msgType}");
            #endif

            // キーの位置を記録
            var keyIndex = (int)reader.Consumed;
            reader.Skip(); // キーをスキップ
            var keyOffset = (int)reader.Consumed;
            
            // 値の位置
            reader.Skip(); // 値をスキップ
            
            #if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[MessageBuilder] Key range: {keyIndex}-{keyOffset} (length: {keyOffset-keyIndex})");
            
            // キー部分のバイト列を表示
            var keyBytes = new byte[keyOffset - keyIndex];
            Array.Copy(buffer, keyIndex, keyBytes, 0, keyBytes.Length);
            UnityEngine.Debug.Log($"[MessageBuilder] Key bytes: {BitConverter.ToString(keyBytes)}");
            
            // ハッシュコードを計算して表示
            uint hash = 2166136261;
            for (int i = 0; i < keyBytes.Length; i++)
            {
                hash = unchecked((keyBytes[i] ^ hash) * 16777619);
            }
            UnityEngine.Debug.Log($"[MessageBuilder] Key hash: {unchecked((int)hash)}");
            #endif

            return new InterprocessMessage(msgType, buffer, keyIndex, keyOffset);
        }
        
    }

    // AsyncKeyEqualityComparer クラスを追加して等価性比較を強化
    // MessageBuilder.cs 内にAsyncKeyEqualityComparerクラスを追加
    [Preserve]
    internal class AsyncKeyEqualityComparer : IEqualityComparer<IInterprocessKey>
    {
        [Preserve]
        public bool Equals(IInterprocessKey x, IInterprocessKey y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x == null || y == null) return false;
        
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
        UnityEngine.Debug.Log($"[AsyncKeyEqualityComparer] Comparing keys");
#endif
        
            var xSpan = x.KeyMemory.Span;
            var ySpan = y.KeyMemory.Span;
        
            if (xSpan.Length != ySpan.Length)
            {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
            UnityEngine.Debug.Log($"[AsyncKeyEqualityComparer] Key length mismatch: {xSpan.Length} != {ySpan.Length}");
#endif
                return false;
            }
        
            for (int i = 0; i < xSpan.Length; i++)
            {
                if (xSpan[i] != ySpan[i])
                {
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
                UnityEngine.Debug.Log($"[AsyncKeyEqualityComparer] Byte mismatch at index {i}: {xSpan[i]} != {ySpan[i]}");
#endif
                    return false;
                }
            }
        
#if MESSAGEPIPE_TCP_RECEIVE_DEBUG
        UnityEngine.Debug.Log($"[AsyncKeyEqualityComparer] Keys are equal");
#endif
            return true;
        }
    
        [Preserve]
        public int GetHashCode(IInterprocessKey obj)
        {
            if (obj == null) return 0;
        
            // FNV1A32 ハッシュアルゴリズム
            uint hash = 2166136261;
            var span = obj.KeyMemory.Span;
        
            for (int i = 0; i < span.Length; i++)
            {
                hash = unchecked((span[i] ^ hash) * 16777619);
            }
        
            return unchecked((int)hash);
        }
    }
    
    // (messageId:int, (reqType,resType):(string,string))

    [Preserve]
    [MessagePackFormatter(typeof(Formatter))]
    internal class RequestHeader
    {
        public int MessageId { get; }
        public string RequestType { get; }
        public string ResponseType { get; }

        public RequestHeader(int messageId, string requestType, string responseType)
        {
            MessageId = messageId;
            RequestType = requestType;
            ResponseType = responseType;
        }

        [Preserve]
        public class Formatter : IMessagePackFormatter<RequestHeader>
        {
            public RequestHeader Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
            {
                // debugging...
                var x = reader.ReadArrayHeader();
                if (x != 3) throw new MessagePack.MessagePackSerializationException("Array length is invalid. Length:" + x);
                var id = reader.ReadInt32();

                var req = reader.ReadString();
                var res = reader.ReadString();
                return new RequestHeader(id, req, res);
            }

            public void Serialize(ref MessagePackWriter writer, RequestHeader value, MessagePackSerializerOptions options)
            {
                writer.WriteArrayHeader(3);
                writer.Write(value.MessageId);
                writer.Write(value.RequestType);
                writer.Write(value.ResponseType);
            }
        }
    }
}
