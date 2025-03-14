using System;

namespace MessagePipe.Interprocess.Extended
{
    /// <summary>
    /// TCP送信用のメッセージコンテナ
    /// </summary>
    internal class TcpMessageContainer
    {
        /// <summary>
        /// メッセージID（追跡用）
        /// </summary>
        public string MessageId { get; set; } = Guid.NewGuid().ToString();

        /// <summary>
        /// 送信データ
        /// </summary>
        public byte[] Data { get; set; }

        /// <summary>
        /// 送信先アドレス
        /// </summary>
        public string ToAddress { get; set; }

        /// <summary>
        /// 送信先ポート
        /// </summary>
        public int? Port { get; set; }

        /// <summary>
        /// リトライ回数
        /// </summary>
        public int RetryCount { get; set; } = 3;

        /// <summary>
        /// タイムアウト時間
        /// </summary>
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// 送信時刻
        /// </summary>
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;

        /// <summary>
        /// 現在のリトライ回数
        /// </summary>
        public int CurrentRetryCount { get; set; } = 0;

        /// <summary>
        /// エラー発生時のコールバック
        /// </summary>
        public Action<Exception> ErrorCallback { get; set; }

        /// <summary>
        /// 送信完了時のコールバック
        /// </summary>
        public Action CompletionCallback { get; set; }

        /// <summary>
        /// 送信状態
        /// </summary>
        public TcpMessageState State { get; set; } = TcpMessageState.Pending;
    }

    /// <summary>
    /// メッセージの送信状態
    /// </summary>
    public enum TcpMessageState
    {
        /// <summary>
        /// 送信待ち
        /// </summary>
        Pending,

        /// <summary>
        /// 送信中
        /// </summary>
        Sending,

        /// <summary>
        /// 送信完了
        /// </summary>
        Completed,

        /// <summary>
        /// 送信失敗
        /// </summary>
        Failed,

        /// <summary>
        /// リトライ中
        /// </summary>
        Retrying
    }
}
