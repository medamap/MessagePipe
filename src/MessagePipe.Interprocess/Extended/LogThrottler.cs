using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace MessagePipe.Interprocess
{
    /// <summary>
    /// ログスロットリング用のヘルパークラス
    /// </summary>
    public static class LogThrottler
    {
        // 通常のDictionaryからConcurrentDictionaryに変更
        private static readonly ConcurrentDictionary<string, ThrottleInfo> ThrottleInfos = new ConcurrentDictionary<string, ThrottleInfo>();
        
        private class ThrottleInfo
        {
            public int Counter { get; set; }
            public DateTime LastLogTime { get; set; }
            public int SkippedCount { get; set; }
        }
        
        /// <summary>
        /// スロットリングされたログを出力します
        /// </summary>
        public static void ThrottledLog(string key, Action<int> logAction, int threshold = 100, int timeThresholdMs = 1000)
        {
            // GetOrAddを使用して安全に取得または作成
            var info = ThrottleInfos.GetOrAdd(key, _ => new ThrottleInfo());
            
            // 複数スレッドからのアクセスを考慮してlockで保護
            lock (info)
            {
                info.Counter++;
                info.SkippedCount++;
                
                // 閾値を超えたか、最後のログから一定時間経過した場合
                if (info.Counter >= threshold || 
                    (DateTime.UtcNow - info.LastLogTime).TotalMilliseconds >= timeThresholdMs)
                {
                    // スキップされたログの数を含めて出力
                    try
                    {
                        logAction(info.SkippedCount);
                    }
                    catch (Exception ex)
                    {
                        #if UNITY_2018_3_OR_NEWER
                        UnityEngine.Debug.LogError("[LogThrottler] Error in logging action: " + ex.Message);
                        #endif
                    }
                    
                    // リセット
                    info.Counter = 0;
                    info.SkippedCount = 0;
                    info.LastLogTime = DateTime.UtcNow;
                }
            }
        }
    }
}