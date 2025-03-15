# 魔改造版 MessagePipe.Interprocess

## MessagePipe魔改造版とDI（VContainer）の関係

今回の魔改造版でも、MessagePipeの基本的なDIの仕組みは変わっていません。VContainerを使った依存性注入は引き続き同じように行えます。

### 魔改造版のDI対応

1. 基本的なDIの流れは同じ
   - VContainerでサービスを登録
   - コンストラクタインジェクションでパブリッシャー/サブスクライバーを取得
   - 通常のMessagePipeと同じインターフェースを使用

1. 拡張機能へのアクセス方法

基本インターフェース（
```csharp
IDistributedPublisher<TKey, TMessage>
```
で DI

拡張機能を使う場合は具象クラス（
```csharp
TcpDistributedPublisher<TKey, TMessage>
```
にキャスト

### 実装例

1. VContainerでの登録

```csharp
public void Configure(IContainerBuilder builder)
{
    // MessagePipeの基本設定
    var options = builder.RegisterMessagePipe();
    
    // TCP拡張オプションの設定
    var tcpOptions = new MessagePipeInterprocessTcpExtendedOptions("127.0.0.1", 12345)
    {
        IgnoreConnectErrors = true,
        IgnoreSendErrors = true,
        MaxRetryCount = 3
    };
    
    // TCP通信の登録
    builder.RegisterTcpInterprocessMessageBroker<string, DataMessage>(tcpOptions);
    
    // サービスの登録
    builder.Register<DataService>(Lifetime.Singleton);
}
```

2. サービスでのDI

```csharp
public class DataService
{
    private readonly IDistributedPublisher<string, DataMessage> _publisher;
    private readonly TcpDistributedPublisher<string, DataMessage> _tcpPublisher;
    
    // 通常のDIパターン - インターフェースで注入
    public DataService(
        IDistributedPublisher<string, DataMessage> publisher)
    {
        _publisher = publisher;
        // 拡張機能を使う場合はキャスト
        _tcpPublisher = publisher as TcpDistributedPublisher<string, DataMessage>;
    }
    
    // 基本的な送信（通常のMessagePipeと同じ）
    public async UniTask SendBasic(DataMessage message)
    {
        await _publisher.PublishAsync("data/message", message);
    }
    
    // 拡張機能を使った送信
    public async UniTask SendAdvanced(DataMessage message, string targetAddress)
    {
        if (_tcpPublisher != null)
        {
            // 方法1: 拡張メソッドを直接使用
            await _tcpPublisher.PublishToTargetAsync(
                "data/message", 
                message, 
                targetAddress);
                
            // 方法2: Fluent APIを使用
            await _tcpPublisher.CreatePublisher()
                .WithTarget(targetAddress)
                .WithRetry(3)
                .PublishAsync("data/message", message);
        }
        else
        {
            // TCP拡張が使えない場合は通常の送信
            await _publisher.PublishAsync("data/message", message);
        }
    }
}
```
3. VContainerでのコンポーネント注入

```csharp
public class DataController : MonoBehaviour
{
    [Inject] private IDistributedPublisher<string, DataMessage> _publisher;
    [Inject] private IDistributedSubscriber<string, DataMessage> _subscriber;
    
    private TcpDistributedPublisher<string, DataMessage> _tcpPublisher;
    
    [Inject]
    public void Construct(
        IDistributedPublisher<string, DataMessage> publisher,
        IDistributedSubscriber<string, DataMessage> subscriber)
    {
        _publisher = publisher;
        _subscriber = subscriber;
        _tcpPublisher = publisher as TcpDistributedPublisher<string, DataMessage>;
    }
    
    private void Start()
    {
        // サブスクライブ（通常のMessagePipeと同じ）
        _subscriber.SubscribeAsync(
            "data/message",
            message => HandleData(message)
        ).Forget();
    }
    
    // 拡張機能を使った送信
    public void SendWithFluentAPI(DataMessage message, string targetAddress)
    {
        if (_tcpPublisher != null)
        {
            _tcpPublisher.CreatePublisher()
                .WithTarget(targetAddress)
                .WithErrorCallback(ex => Debug.LogError($"送信エラー: {ex.Message}"))
                .Publish("data/message", message);
        }
        else
        {
            _publisher.PublishAsync("data/message", message).Forget();
        }
    }
}
```

### まとめ

1. DIの基本は変わらない
   - VContainerを使ったDIの基本的な流れは同じ
   - VContainerでの登録方法も基本的に同じ
 
1. 拡張機能の使い方
   - 基本インターフェースでDIして、必要に応じて具象クラスにキャスト
   - キャストできた場合のみ拡張機能を使用する条件分岐を入れると安全

1. 後方互換性
   - 既存のコードはそのまま動作する
   - 拡張機能は必要な箇所だけ段階的に導入可能

この方法により、既存のDIの仕組みを活かしながら、魔改造版の拡張機能も利用できます。

## TCP魔改造版の導入方法

1. 初期化部分の変更

```csharp
// TCP メッセージ送受信用のオプションを設定
MessagePipeInterprocessTcpExtendedOptions tcpOptions =
new MessagePipeInterprocessTcpExtendedOptions(
    "192.168.10.10", // 送信先IPアドレス
    12345) // ポート
{
    IgnoreConnectErrors = true, // 接続エラーを無視
    IgnoreSendErrors = true,    // 送信エラーを無視
    MaxRetryCount = 3,          // 最大リトライ回数
    ConnectionTimeoutMs = 5000, // 接続タイムアウト(ms)
    SendTimeoutMs = 3000,       // 送信タイムアウト(ms)
    MessagePackSerializerOptions = serializerOptions // 既存のシリアライザオプション
};

// メッセージブローカー登録
builder.
    ToMessagePipeBuilder().
    RegisterUpdInterprocessMessageBroker<string, DataMessage>(options);
```

2. 送信側の実装例（簡略版）

```csharp
// Fluent APIを使用した送信
await _publisher.CreatePublisher()
    .WithTarget(targetAddress)
    .WithRetry(3)
    .WithTimeout(TimeSpan.FromSeconds(2))
    .WithErrorCallback(ex => {
        Debug.LogError($"データの送信に失敗: {ex.Message}");
    })
    .WaitForCompletion()
    .PublishAsync("data/message", message);
```

3. 受信側の実装例（簡略版）

```csharp
// メッセージのサブスクライブ
await _subscriber.SubscribeAsync(
    "data/message",
    message => {
        Debug.Log($"データを受信: ID={message.InstanceId}, 送信元={message.FromAddress}");
    }
);
```

4.  IToAddressableインターフェースの実装

```csharp
// 例: DataMessage.cs
[MessagePackObject]
public struct DataMessage : IToAddressable
{
    [Key(0)] public string FromUuid;
    [Key(1)] public string FromAddress;
    [Key(2)] public string ToAddress;
    // 他のフィールド...
    
    // IToAddressableインターフェースの実装
    public string GetToAddress() => ToAddress;
}
```

5. メッセージブローカー登録メソッドの修正

```csharp
///  summary InteriorViewerのカスタムメッセージブローカを登録 /summary 
public static void RegisterDataInterprocessMessageBroker(this IContainerBuilder builder, MessagePipeInterprocessOptions options)
{
    // TCP版を使用
    if (options is MessagePipeInterprocessTcpExtendedOptions ||
        options is MessagePipeInterprocessTcpUdsExtendedOptions)
    {
        builder.ToMessagePipeBuilder().RegisterTcpInterprocessMessageBroker string, DataMessage (options);
        // 他のメッセージタイプも同様に...
    }
    // 既存のUDP版を使用
    else
    {
        builder.ToMessagePipeBuilder().RegisterUpdInterprocessMessageBroker string, DataMessage (options);
        // 他のメッセージタイプも同様に...
    }
}
```

6. IToAddressableインターフェースの実装

既存のメッセージクラスに
```csharp
IToAddressable
```
インターフェースを実装します：

```csharp
// 例: FurnitureTransformMessage.cs
[MessagePackObject]
public struct DataMessage : IToAddressable
{
    [Key(0)] public string FromUuid;
    [Key(1)] public string FromAddress;
    [Key(2)] public string ToAddress;
    // 他のフィールド...
    
    // IToAddressableインターフェースの実装
    public string GetToAddress() =  ToAddress;
}
```

7. 送信側の実装例

```csharp
/// <summary>データAPI</summary>
public class DataApi
{
    /// <summary>TCP版データ送</summary>
    private readonly TcpDistributedPublisher<string, DataMessage> _dataPublisher;

    /// <summary>依存注入</summary>
    public DataApi(
        IDistributedPublisher<string, DataMessage> dataPublisher,
    ) {
        _dataPublisher = dataPublisher as TcpDistributedPublisher<string, DataMessage>;
    }
    
    // データ送信
    public async UniTask SendData(string instanceId, string targetAddress)
    {
        var message = new DataMessage(
            SystemInfo.deviceUniqueIdentifier, // 送信元UUID
            "192.168.10.10", // 送信元アドレス
            targetAddress,   // 送信先アドレス
            instanceId,
        );
        
        try
        {
            // Fluent APIを使用した送信
            await _dataPublisher.CreatePublisher()
                .WithTarget(targetAddress)
                .WithRetry(3)
                .WithTimeout(TimeSpan.FromSeconds(2))
                .WithErrorCallback(ex => {
                    Debug.LogError($"データの送信に失敗: {ex.Message}");
                })
                .WaitForCompletion()
                .PublishAsync("data/message", message);
                
            Debug.Log($ データを送信: ID={instanceId}, 宛先={targetAddress} );
        }
        catch (Exception ex)
        {
            Debug.LogError($"データの送信中にエラー: {ex.Message}");
            // エラー処理
        }
    }
}
```

8. 受信側の実装例

```csharp
public class DataReceiver : IAsyncStartable
{
    private readonly IDistributedSubscriber<string, DataMessage> _dataSubscriber;
    private readonly CompositeDisposable _disposables = new CompositeDisposable();
    
    public DataReceiver(
        IDistributedSubscriber<string, DataMessage> dataSubscriber,
    ) {
        _dataSubscriber = dataSubscriber;
    }
    
    // 受信の開始
    public async UniTask StartReceiving(CancellationToken ct)
    {
        // メッセージのサブスクライブ
        await _dataSubscriber.SubscribeAsync(
            "data/message",
            message => {
                Debug.Log($"データ受信: ID={message.InstanceId}, 送信元={message.FromAddress}");
                );
            },
            message => message.GetToAddress().Equals(localIpAddress), // 自分宛のメッセージのみ受信
            cancellationToken: ct
        );
    }
}
```

9. UDP/TCP切り替え機能の実装

```csharp
// 設定に基づいてメッセージングオプションを作成
MessagePipeInterprocessOptions CreateMessagingOptions(NetworkSettings settings, MessagePackSerializerOptions serializerOptions)
{
    if (settings.UseTcp)
    {
        // TCP通信を使用
        return new MessagePipeInterprocessTcpExtendedOptions(
            settings.LocalIPAddress,
            settings.Port)
        {
            IgnoreConnectErrors = settings.IgnoreConnectionErrors,
            IgnoreSendErrors = settings.IgnoreSendErrors,
            MaxRetryCount = settings.MaxRetryCount,
            ConnectionTimeoutMs = settings.ConnectionTimeoutMs,
            MessagePackSerializerOptions = serializerOptions
        };
    }
    else
    {
        // UDP通信を使用
        return builder.ToMessagePipeBuilder()
            .AddUdpInterprocessWithSubnet(
                settings.BroadcastAddress,
                settings.Port,
                settings.SubnetMask,
                settings.NetworkAddress,
                settings.IgnoreBindErrors,
                settings.IgnoreSendErrors,
                opt =  {
                    opt.MessagePackSerializerOptions = serializerOptions;
                });
    }
}
```
