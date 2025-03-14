using System;
using MessagePack;
using MessagePack.Resolvers;

namespace MessagePipe.Interprocess.Extended
{
    ///  summary 
    /// TCP通信の拡張オプション
    ///  /summary 
    public sealed class MessagePipeInterprocessTcpExtendedOptions : MessagePipeInterprocessTcpOptions
    {
        ///  summary 
        /// TCP接続エラーを無視するかどうか
        /// true の場合、接続に失敗しても例外をスローせず、静かに失敗します
        /// false の場合、接続に失敗すると例外がスローされます（デフォルト）
        ///  /summary 
        public bool IgnoreConnectErrors { get; set; } = false;

        ///  summary 
        /// ネットワーク送信エラーを無視するかどうか
        /// true の場合、送信エラーが発生しても処理を継続します
        /// false の場合、送信エラーが発生するとパブリッシュループが終了します（デフォルト）
        ///  /summary 
        public bool IgnoreSendErrors { get; set; } = false;

        ///  summary 
        /// 接続タイムアウト（ミリ秒）
        ///  /summary 
        public int ConnectionTimeoutMs { get; set; } = 5000;

        ///  summary 
        /// 送信タイムアウト（ミリ秒）
        ///  /summary 
        public int SendTimeoutMs { get; set; } = 5000;

        ///  summary 
        /// 最大リトライ回数
        ///  /summary 
        public int MaxRetryCount { get; set; } = 3;

        ///  summary 
        /// リトライ間隔（ミリ秒）
        ///  /summary 
        public int RetryIntervalMs { get; set; } = 500;

        ///  summary 
        /// アイドル接続のタイムアウト（ミリ秒）
        ///  /summary 
        public int IdleConnectionTimeoutMs { get; set; } = 300000; // 5分

        ///  summary 
        /// 接続プールのクリーンアップ間隔（ミリ秒）
        ///  /summary 
        public int ConnectionPoolCleanupIntervalMs { get; set; } = 60000; // 1分

        ///  summary 
        /// コンストラクタ
        ///  /summary 
        public MessagePipeInterprocessTcpExtendedOptions(string host, int port)
            : base(host, port)
        {
        }

        ///  summary 
        /// 接続タイムアウトをTimeSpanで取得
        ///  /summary 
        public TimeSpan ConnectionTimeout => TimeSpan.FromMilliseconds(ConnectionTimeoutMs);

        ///  summary 
        /// 送信タイムアウトをTimeSpanで取得
        ///  /summary 
        public TimeSpan SendTimeout => TimeSpan.FromMilliseconds(SendTimeoutMs);

        ///  summary 
        /// リトライ間隔をTimeSpanで取得
        ///  /summary 
        public TimeSpan RetryInterval => TimeSpan.FromMilliseconds(RetryIntervalMs);

        ///  summary 
        /// アイドル接続のタイムアウトをTimeSpanで取得
        ///  /summary 
        public TimeSpan IdleConnectionTimeout => TimeSpan.FromMilliseconds(IdleConnectionTimeoutMs);

        ///  summary 
        /// 接続プールのクリーンアップ間隔をTimeSpanで取得
        ///  /summary 
        public TimeSpan ConnectionPoolCleanupInterval => TimeSpan.FromMilliseconds(ConnectionPoolCleanupIntervalMs);
    }

#if NET5_0_OR_GREATER
    ///  summary 
    /// TCP UDS通信の拡張オプション
    ///  /summary 
    public sealed class MessagePipeInterprocessTcpUdsExtendedOptions : MessagePipeInterprocessTcpUdsOptions
    {
        ///  summary 
        /// TCP接続エラーを無視するかどうか
        ///  /summary 
        public bool IgnoreConnectErrors { get; set; } = false;

        ///  summary 
        /// ネットワーク送信エラーを無視するかどうか
        ///  /summary 
        public bool IgnoreSendErrors { get; set; } = false;

        ///  summary 
        /// 接続タイムアウト（ミリ秒）
        ///  /summary 
        public int ConnectionTimeoutMs { get; set; } = 5000;

        ///  summary 
        /// 送信タイムアウト（ミリ秒）
        ///  /summary 
        public int SendTimeoutMs { get; set; } = 5000;

        ///  summary 
        /// 最大リトライ回数
        ///  /summary 
        public int MaxRetryCount { get; set; } = 3;

        ///  summary 
        /// リトライ間隔（ミリ秒）
        ///  /summary 
        public int RetryIntervalMs { get; set; } = 500;

        ///  summary 
        /// アイドル接続のタイムアウト（ミリ秒）
        ///  /summary 
        public int IdleConnectionTimeoutMs { get; set; } = 300000; // 5分

        ///  summary 
        /// 接続プールのクリーンアップ間隔（ミリ秒）
        ///  /summary 
        public int ConnectionPoolCleanupIntervalMs { get; set; } = 60000; // 1分

        ///  summary 
        /// コンストラクタ
        ///  /summary 
        public MessagePipeInterprocessTcpUdsExtendedOptions(string socketPath)
            : base(socketPath)
        {
        }

        ///  summary 
        /// コンストラクタ
        ///  /summary 
        public MessagePipeInterprocessTcpUdsExtendedOptions(string socketPath, int? sendBufferSize, int? recvBufferSize)
            : base(socketPath, sendBufferSize, recvBufferSize)
        {
        }

        ///  summary 
        /// 接続タイムアウトをTimeSpanで取得
        ///  /summary 
        public TimeSpan ConnectionTimeout => TimeSpan.FromMilliseconds(ConnectionTimeoutMs);

        ///  summary 
        /// 送信タイムアウトをTimeSpanで取得
        ///  /summary 
        public TimeSpan SendTimeout => TimeSpan.FromMilliseconds(SendTimeoutMs);

        ///  summary 
        /// リトライ間隔をTimeSpanで取得
        ///  /summary 
        public TimeSpan RetryInterval => TimeSpan.FromMilliseconds(RetryIntervalMs);

        ///  summary 
        /// アイドル接続のタイムアウトをTimeSpanで取得
        ///  /summary 
        public TimeSpan IdleConnectionTimeout => TimeSpan.FromMilliseconds(IdleConnectionTimeoutMs);

        ///  summary 
        /// 接続プールのクリーンアップ間隔をTimeSpanで取得
        ///  /summary 
        public TimeSpan ConnectionPoolCleanupInterval => TimeSpan.FromMilliseconds(ConnectionPoolCleanupIntervalMs);
    }
#endif
}
