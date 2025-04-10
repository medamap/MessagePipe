using System;
using System.Collections.Generic;

// ReSharper disable CheckNamespace

namespace MessagePipe.Interprocess
{
    /// <summary>
    /// ログスロットリング用のヘルパークラス
    /// </summary>
    public static class LogThrottler
    {
        private static readonly Dictionary<string, ThrottleInfo> ThrottleInfos = new ();
        
        private class ThrottleInfo
        {
            public int Counter { get; set; }
            public DateTime LastLogTime { get; set; }
            public int SkippedCount { get; set; }
        }
        
        /// <summary>
        /// スロットリングされたログを出力します
        /// </summary>
        /// <param name="key">スロットリング用のキー</param>
        /// <param name="logAction">ログ出力アクション</param>
        /// <param name="threshold">出力閾値</param>
        /// <param name="timeThresholdMs">時間閾値（ミリ秒）</param>
        public static void ThrottledLog(string key, Action<int> logAction, int threshold = 100, int timeThresholdMs = 1000)
        {
            if (!ThrottleInfos.TryGetValue(key, out var info))
            {
                info = new ThrottleInfo();
                ThrottleInfos[key] = info;
            }
            
            info.Counter++;
            info.SkippedCount++;
            
            // 閾値を超えたか、最後のログから一定時間経過した場合
            if (info.Counter >= threshold || 
                (DateTime.UtcNow - info.LastLogTime).TotalMilliseconds >= timeThresholdMs)
            {
                // スキップされたログの数を含めて出力
                logAction(info.SkippedCount);
                
                // リセット
                info.Counter = 0;
                info.SkippedCount = 0;
                info.LastLogTime = DateTime.UtcNow;
            }
        }
    }
}