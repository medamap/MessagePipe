using System;
using System.Threading;
using Cysharp.Threading.Tasks;
using MessagePipe.Interprocess.Workers;

namespace MessagePipe.Interprocess
{
    /// <summary>
    /// Fluent APIを提供するUDPパブリッシャー
    /// </summary>
    public class FluentUdpPublisher
    {
        private readonly UdpWorker _worker;
        private readonly MessagePipeInterprocessOptions _options;
        
        private string _targetAddress;
        private int? _targetPort;
        private Action<Exception> _errorCallback;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        public FluentUdpPublisher(UdpWorker worker, MessagePipeInterprocessOptions options)
        {
            _worker = worker;
            _options = options;
            
            // 初期値を設定
            if (options is MessagePipeInterprocessUdpOptions udpOptions)
            {
                _targetAddress = udpOptions.Host;
                _targetPort = udpOptions.Port;
            }
        }

        /// <summary>
        /// 送信先アドレスを指定
        /// </summary>
        public FluentUdpPublisher WithTarget(string address)
        {
            _targetAddress = address;
            return this;
        }

        /// <summary>
        /// 送信先ポートを指定
        /// </summary>
        public FluentUdpPublisher WithPort(int port)
        {
            _targetPort = port;
            return this;
        }

        /// <summary>
        /// エラー発生時のコールバックを指定
        /// </summary>
        public FluentUdpPublisher WithErrorCallback(Action<Exception> callback)
        {
            _errorCallback = callback;
            return this;
        }

        /// <summary>
        /// メッセージを非同期で送信
        /// </summary>
        public async UniTask PublishAsync<TKey, TMessage>(TKey key, TMessage message, CancellationToken cancellationToken = default)
        {
            // 送信先アドレスの取得
            string targetAddress = _targetAddress;
            int? targetPort = _targetPort;
            
            // メッセージがIToAddressableを実装している場合は、そのアドレスを使用
            if (message is IToAddressable addressable)
            {
                string messageAddress = addressable.GetToAddress();
                if (!string.IsNullOrEmpty(messageAddress))
                {
                    targetAddress = messageAddress;
                }
            }
            else if (message is IToAdressable oldAddressable) // 古いインターフェースもサポート
            {
                string messageAddress = oldAddressable.GetToAddress();
                if (!string.IsNullOrEmpty(messageAddress))
                {
                    targetAddress = messageAddress;
                }
            }
            
            // メッセージがIToPortableを実装している場合は、そのポートを使用
            if (message is IToPortable portable)
            {
                int messagePort = portable.GetPort();
                if (messagePort > 0)
                {
                    targetPort = messagePort;
                }
            }
            
            try
            {
                // 送信先アドレスとポートの両方が指定されている場合
                if (!string.IsNullOrEmpty(targetAddress) && targetPort.HasValue)
                {
                    _worker.PublishToTarget(key, message, targetAddress, targetPort.Value);
                }
                // 送信先アドレスのみ指定されている場合
                else if (!string.IsNullOrEmpty(targetAddress))
                {
                    _worker.Publish(key, message, targetAddress);
                }
                // デフォルト送信先を使用
                else
                {
                    _worker.Publish(key, message);
                }
            }
            catch (Exception ex)
            {
                if (_errorCallback != null)
                {
                    _errorCallback(ex);
                }
                else
                {
                    bool ignoreErrors = _options is MessagePipeInterprocessUdpOptions udpOptions && udpOptions.IgnoreSendErrors;
                    if (!ignoreErrors)
                    {
                        throw;
                    }
                    else
                    {
                        _options.UnhandledErrorHandler?.Invoke(
                            $"Failed to publish UDP message to {targetAddress}:{targetPort} but continuing due to IgnoreSendErrors option.",
                            ex);
                    }
                }
            }
        }

        /// <summary>
        /// メッセージを同期的に送信（内部的には非同期で処理）
        /// </summary>
        public void Publish<TKey, TMessage>(TKey key, TMessage message)
        {
            PublishAsync(key, message).Forget();
        }
    }
}
