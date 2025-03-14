using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cysharp.Threading.Tasks;
using MessagePipe.Interprocess.Workers;

namespace MessagePipe.Interprocess
{
    /// <summary>
    /// TCP接続情報を保持するクラス
    /// </summary>
    public class TcpConnectionInfo
    {
        /// <summary>
        /// 接続先アドレス
        /// </summary>
        public string Address { get; }

        /// <summary>
        /// 接続先ポート
        /// </summary>
        public int Port { get; }

        /// <summary>
        /// TCP接続クライアント
        /// </summary>
        public SocketTcpClient Client { get; }

        /// <summary>
        /// 最終アクセス時刻
        /// </summary>
        public DateTime LastAccessTime { get; private set; }

        /// <summary>
        /// 接続が有効かどうか
        /// </summary>
        public bool IsValid { get; private set; }

        /// <summary>
        /// コンストラクタ
        /// </summary>
        public TcpConnectionInfo(string address, int port, SocketTcpClient client)
        {
            Address = address;
            Port = port;
            Client = client;
            LastAccessTime = DateTime.UtcNow;
            IsValid = true;
        }

        /// <summary>
        /// 最終アクセス時刻を更新
        /// </summary>
        public void UpdateLastAccessTime()
        {
            LastAccessTime = DateTime.UtcNow;
        }

        /// <summary>
        /// 接続を無効化
        /// </summary>
        public void Invalidate()
        {
            IsValid = false;
        }
    }

    /// <summary>
    /// TCP接続を管理するプールクラス
    /// </summary>
    public class TcpConnectionPool : IDisposable
    {
        private readonly ConcurrentDictionary<string, TcpConnectionInfo> _connections = new ConcurrentDictionary<string, TcpConnectionInfo>();
        private readonly TimeSpan _connectionTimeout;
        private readonly TimeSpan _cleanupInterval;
        private readonly CancellationTokenSource _cleanupCts = new CancellationTokenSource();
        private readonly MessagePipeInterprocessOptions _options;
        private bool _isDisposed = false;

        /// <summary>
        /// コンストラクタ
        /// </summary>
        public TcpConnectionPool(MessagePipeInterprocessOptions options, TimeSpan? connectionTimeout = null, TimeSpan? cleanupInterval = null)
        {
            _options = options;
            _connectionTimeout = connectionTimeout ?? TimeSpan.FromMinutes(5);
            _cleanupInterval = cleanupInterval ?? TimeSpan.FromMinutes(1);
            
            // 定期的なクリーンアップタスクを開始
            StartCleanupTask();
        }

        /// <summary>
        /// 接続を取得または作成
        /// </summary>
        public async UniTask<SocketTcpClient> GetOrCreateConnectionAsync(string address, int port, CancellationToken cancellationToken = default)
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(TcpConnectionPool));
            }

            var key = GetConnectionKey(address, port);
            
            // 既存の接続を確認
            if (_connections.TryGetValue(key, out var connectionInfo) && connectionInfo.IsValid)
            {
                connectionInfo.UpdateLastAccessTime();
                return connectionInfo.Client;
            }

            // 新しい接続を作成
            try
            {
                var client = SocketTcpClient.Connect(address, port);
                var newConnectionInfo = new TcpConnectionInfo(address, port, client);
                _connections[key] = newConnectionInfo;
                return client;
            }
            catch (SocketException ex)
            {
                // 接続エラーの処理
                var tcpOptions = _options as MessagePipeInterprocessTcpOptions;
                bool ignoreErrors = tcpOptions != null && 
                                   (tcpOptions is MessagePipeInterprocessTcpExtendedOptions extendedOptions && 
                                    extendedOptions.IgnoreConnectErrors);

                if (ignoreErrors)
                {
                    _options.UnhandledErrorHandler?.Invoke($"Failed to connect to {address}:{port}, but continuing due to IgnoreConnectErrors option.", ex);
                    return null;
                }
                
                throw new InvalidOperationException($"Failed to connect to {address}:{port}. This may be due to network issues or the target server not being available.", ex);
            }
        }

        /// <summary>
        /// 接続を閉じる
        /// </summary>
        public void CloseConnection(string address, int port)
        {
            var key = GetConnectionKey(address, port);
            if (_connections.TryRemove(key, out var connectionInfo))
            {
                try
                {
                    connectionInfo.Client.Dispose();
                }
                catch (Exception ex)
                {
                    _options.UnhandledErrorHandler?.Invoke($"Error closing connection to {address}:{port}", ex);
                }
            }
        }

        /// <summary>
        /// 接続キーを生成
        /// </summary>
        private string GetConnectionKey(string address, int port)
        {
            return $"{address}:{port}";
        }

        /// <summary>
        /// クリーンアップタスクを開始
        /// </summary>
        private void StartCleanupTask()
        {
            RunCleanupLoop().Forget();
        }

        /// <summary>
        /// 定期的なクリーンアップループ
        /// </summary>
        private async UniTask RunCleanupLoop()
        {
            var token = _cleanupCts.Token;
            
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await UniTask.Delay(_cleanupInterval, cancellationToken: token);
                    CleanupIdleConnections();
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _options.UnhandledErrorHandler?.Invoke("Error in connection pool cleanup task", ex);
                }
            }
        }

        /// <summary>
        /// アイドル状態の接続をクリーンアップ
        /// </summary>
        private void CleanupIdleConnections()
        {
            var now = DateTime.UtcNow;
            var keysToRemove = new List<string>();
            
            foreach (var kvp in _connections)
            {
                if (now - kvp.Value.LastAccessTime > _connectionTimeout)
                {
                    keysToRemove.Add(kvp.Key);
                }
            }
            
            foreach (var key in keysToRemove)
            {
                if (_connections.TryRemove(key, out var connectionInfo))
                {
                    try
                    {
                        connectionInfo.Client.Dispose();
                    }
                    catch (Exception ex)
                    {
                        _options.UnhandledErrorHandler?.Invoke($"Error disposing connection: {key}", ex);
                    }
                }
            }
        }

        /// <summary>
        /// リソースの解放
        /// </summary>
        public void Dispose()
        {
            if (_isDisposed)
                return;

            _isDisposed = true;
            _cleanupCts.Cancel();
            _cleanupCts.Dispose();
            
            foreach (var connectionInfo in _connections.Values)
            {
                try
                {
                    connectionInfo.Client.Dispose();
                }
                catch (Exception ex)
                {
                    _options.UnhandledErrorHandler?.Invoke($"Error disposing connection to {connectionInfo.Address}:{connectionInfo.Port}", ex);
                }
            }
            
            _connections.Clear();
        }
    }
}
