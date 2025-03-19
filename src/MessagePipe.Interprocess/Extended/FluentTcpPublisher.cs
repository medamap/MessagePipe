using System;
using System.Threading;
using System.Threading.Tasks;
using Cysharp.Threading.Tasks;
using MessagePipe.Interprocess.Workers;
using MessagePipe.Interprocess.Internal;


namespace MessagePipe.Interprocess
{
    /// <summary>
    /// Fluent APIを提供するTCPパブリッシャー
    /// </summary>
    public class FluentTcpPublisher
    {
        private readonly TcpWorker _worker;
        private readonly TcpConnectionPool _connectionPool;
        private readonly MessagePipeInterprocessOptions _options;
        
        private string _targetAddress;
        private int? _targetPort;
        private int _retryCount = 3;
        private TimeSpan _timeout = TimeSpan.FromSeconds(5);
        private TimeSpan _retryInterval = TimeSpan.FromMilliseconds(500);
        private Action<Exception> _errorCallback;
        private Action _completionCallback;
        private bool _waitForCompletion = true;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        public FluentTcpPublisher(TcpWorker worker, TcpConnectionPool connectionPool, MessagePipeInterprocessOptions options)
        {
            _worker = worker;
            _connectionPool = connectionPool;
            _options = options;
            
            // 拡張オプションから初期値を設定
            if (options is MessagePipeInterprocessTcpExtendedOptions extOptions)
            {
                _retryCount = extOptions.MaxRetryCount;
                _timeout = extOptions.SendTimeout;
                _retryInterval = extOptions.RetryInterval;
            }
            else if (options is MessagePipeInterprocessTcpOptions tcpOptions)
            {
                _targetAddress = tcpOptions.Host;
                _targetPort = tcpOptions.Port;
            }
        }

        /// <summary>
        /// 送信先アドレスを指定
        /// </summary>
        public FluentTcpPublisher WithTarget(string address)
        {
            _targetAddress = address;
            return this;
        }

        /// <summary>
        /// 送信先ポートを指定
        /// </summary>
        public FluentTcpPublisher WithPort(int port)
        {
            _targetPort = port;
            return this;
        }

        /// <summary>
        /// リトライ回数を指定
        /// </summary>
        public FluentTcpPublisher WithRetry(int count)
        {
            _retryCount = count;
            return this;
        }

        /// <summary>
        /// タイムアウトを指定
        /// </summary>
        public FluentTcpPublisher WithTimeout(TimeSpan timeout)
        {
            _timeout = timeout;
            return this;
        }

        /// <summary>
        /// リトライ間隔を指定
        /// </summary>
        public FluentTcpPublisher WithRetryInterval(TimeSpan interval)
        {
            _retryInterval = interval;
            return this;
        }

        /// <summary>
        /// エラー発生時のコールバックを指定
        /// </summary>
        public FluentTcpPublisher WithErrorCallback(Action<Exception> callback)
        {
            _errorCallback = callback;
            return this;
        }

        /// <summary>
        /// 送信完了時のコールバックを指定
        /// </summary>
        public FluentTcpPublisher WithCompletionCallback(Action callback)
        {
            _completionCallback = callback;
            return this;
        }

        /// <summary>
        /// 送信完了を待機するかどうかを指定
        /// </summary>
        public FluentTcpPublisher WaitForCompletion(bool wait = true)
        {
            _waitForCompletion = wait;
            return this;
        }

        /// <summary>
        /// メッセージを非同期で送信
        /// </summary>
        [Preserve]
        public async UniTask PublishAsync<TKey, TMessage>(TKey key, TMessage message, CancellationToken cancellationToken = default)
        {
            // 送信先アドレスの取得
            string targetAddress = _targetAddress;
            int targetPort = _targetPort ?? (_options as MessagePipeInterprocessTcpOptions)?.Port ?? 0;
            
            #if MESSAGEPIPE_TCP_SEND_DEBUG
            UnityEngine.Debug.Log($"[TCP FLUENT] PublishAsync - Key: {key}, Message: {message}, Initial Target: {targetAddress}:{targetPort}");
            #endif
            
            // メッセージがIToAddressableを実装している場合は、そのアドレスを使用
            if (message is IToAddressable addressable)
            {
                string messageAddress = addressable.GetToAddress();
                if (!string.IsNullOrEmpty(messageAddress))
                {
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.Log($"[TCP FLUENT] Using address from IToAddressable: {messageAddress}");
                    #endif
                    targetAddress = messageAddress;
                }
            }
            
            // メッセージがIToPortableを実装している場合は、そのポートを使用
            if (message is IToPortable portable)
            {
                int messagePort = portable.GetPort();
                if (messagePort > 0)
                {
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.Log($"[TCP FLUENT] Using port from IToPortable: {messagePort}");
                    #endif
                    targetPort = messagePort;
                }
            }
            
            // 送信先アドレスとポートの検証
            if (string.IsNullOrEmpty(targetAddress))
            {
                #if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.LogError($"[TCP FLUENT] Target address is not specified");
                #endif
                throw new InvalidOperationException("Target address is not specified.");
            }
            
            if (targetPort <= 0)
            {
                #if MESSAGEPIPE_TCP_SEND_DEBUG
                UnityEngine.Debug.LogError($"[TCP FLUENT] Target port is not specified or invalid: {targetPort}");
                #endif
                throw new InvalidOperationException($"Target port is not specified or invalid: {targetPort}");
            }
            
            #if MESSAGEPIPE_TCP_SEND_DEBUG
            UnityEngine.Debug.Log($"[TCP FLUENT] Final target: {targetAddress}:{targetPort}");
            #endif
            
            // メッセージコンテナの作成
            var messageData = _worker.SerializeMessage(key, message);
            var container = new TcpMessageContainer
            {
                Data = messageData,
                ToAddress = targetAddress,
                Port = targetPort,
                RetryCount = _retryCount,
                Timeout = _timeout,
                ErrorCallback = _errorCallback,
                CompletionCallback = _completionCallback
            };
            
            if (_waitForCompletion)
            {
                // 完了を待機するバージョン
                await SendWithRetryAsync(container, cancellationToken);
            }
            else
            {
                // 非同期で送信（完了を待機しない）
                SendWithRetryAsync(container, cancellationToken).Forget();
            }
        }

        /// <summary>
        /// メッセージを同期的に送信（内部的には非同期で処理）
        /// </summary>
        public void Publish<TKey, TMessage>(TKey key, TMessage message)
        {
            PublishAsync(key, message).Forget();
        }

        /// <summary>
        /// リトライ機能付きの送信処理
        /// </summary>
        [Preserve]
        private async UniTask SendWithRetryAsync(TcpMessageContainer container, CancellationToken cancellationToken)
        {
            #if MESSAGEPIPE_TCP_SEND_DEBUG
            UnityEngine.Debug.Log($"[TCP FLUENT] SendWithRetryAsync - ID: {container.MessageId}, ToAddress: {container.ToAddress}, Port: {container.Port}");
            #endif
            
            int attempts = 0;
            Exception lastException = null;
            
            while (attempts <= container.RetryCount)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.LogWarning($"[TCP FLUENT] Operation canceled");
                    #endif
                    throw new OperationCanceledException("Operation was canceled.");
                }
                
                try
                {
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.Log($"[TCP FLUENT] Attempt {attempts+1}/{container.RetryCount+1} - ID: {container.MessageId}");
                    #endif
                    
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                    cts.CancelAfter(container.Timeout);
                    
                    // 接続を取得または作成
                    var client = await _connectionPool.GetOrCreateConnectionAsync(
                        container.ToAddress, 
                        container.Port.Value, 
                        cts.Token);
                    
                    if (client == null)
                    {
                        #if MESSAGEPIPE_TCP_SEND_DEBUG
                        UnityEngine.Debug.LogError($"[TCP FLUENT] Failed to create connection to {container.ToAddress}:{container.Port}");
                        #endif
                        throw new InvalidOperationException($"Failed to create connection to {container.ToAddress}:{container.Port}");
                    }
                    
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.Log($"[TCP FLUENT] Sending data - Length: {container.Data.Length} bytes");
                    #endif
                    
                    // メッセージを送信
                    await client.SendAsync(container.Data, cts.Token);
                    
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.Log($"[TCP FLUENT] Send completed successfully - ID: {container.MessageId}");
                    #endif
                    
                    // 送信成功
                    container.State = TcpMessageState.Completed;
                    container.CompletionCallback?.Invoke();
                    return;
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.LogWarning($"[TCP FLUENT] Connection timed out - ID: {container.MessageId}");
                    #endif
                    
                    // タイムアウト
                    lastException = new TimeoutException($"Connection to {container.ToAddress}:{container.Port} timed out");
                }
                catch (Exception ex)
                {
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.LogError($"[TCP FLUENT] Error sending message - ID: {container.MessageId}, Error: {ex.Message}\n{ex.StackTrace}");
                    #endif
                    lastException = ex;
                }
                
                attempts++;
                container.CurrentRetryCount = attempts;
                
                // 最大リトライ回数に達していなければ待機して再試行
                if (attempts <= container.RetryCount)
                {
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.Log($"[TCP FLUENT] Retrying after delay - ID: {container.MessageId}, Attempt: {attempts}/{container.RetryCount}");
                    #endif
                    
                    container.State = TcpMessageState.Retrying;
                    await UniTask.Delay(_retryInterval, cancellationToken: cancellationToken);
                }
            }
            
            // すべての試行が失敗
            container.State = TcpMessageState.Failed;
            
            #if MESSAGEPIPE_TCP_SEND_DEBUG
            UnityEngine.Debug.LogError($"[TCP FLUENT] All retry attempts failed - ID: {container.MessageId}, Attempts: {attempts}/{container.RetryCount}");
            #endif
            
            // エラーコールバックを呼び出し
            if (container.ErrorCallback != null)
            {
                container.ErrorCallback(lastException);
            }
            else
            {
                // エラーコールバックが設定されていない場合は例外をスロー
                var ignoreErrors = _options is MessagePipeInterprocessTcpExtendedOptions extOptions && extOptions.IgnoreSendErrors;
                if (!ignoreErrors)
                {
                    throw new InvalidOperationException(
                        $"Failed to publish message to {container.ToAddress}:{container.Port} after {container.RetryCount} attempts", 
                        lastException);
                }
                else
                {
                    // エラーを無視する場合はログに記録
                    #if MESSAGEPIPE_TCP_SEND_DEBUG
                    UnityEngine.Debug.LogWarning($"[TCP FLUENT] Ignoring send errors due to IgnoreSendErrors option - ID: {container.MessageId}");
                    #endif
                    
                    _options.UnhandledErrorHandler?.Invoke(
                        $"Failed to publish message to {container.ToAddress}:{container.Port} after {container.RetryCount} attempts, but continuing due to IgnoreSendErrors option.",
                        lastException);
                }
            }
        }

    }
}
