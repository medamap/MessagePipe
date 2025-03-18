using System;

namespace MessagePipe.Interprocess
{
    /// <summary>
    /// 送信先アドレスを動的に指定するためのインターフェース
    /// </summary>
    public interface IToAddressable
    {
        /// <summary>
        /// 送信先アドレスを取得します
        /// </summary>
        /// <returns>送信先IPアドレスまたはホスト名</returns>
        string GetToAddress();
    }

    /// <summary>
    /// 送信先ポートを動的に指定するためのインターフェース
    /// </summary>
    public interface IToPortable
    {
        /// <summary>
        /// 送信先ポートを取得します
        /// </summary>
        /// <returns>送信先ポート番号</returns>
        int GetPort();
    }

    /// <summary>
    /// 送信先アドレスとポートの両方を動的に指定するためのインターフェース
    /// </summary>
    public interface IToEndpointable : IToAddressable, IToPortable
    {
    }
}   