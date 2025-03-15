# 魔改造版 MessagePipe.Interprocess 完全ガイド

## 目次
1. [概要](#概要)
2. [UDP通信と TCP通信の違い](#udp通信とtcp通信の違い)
3. [基本的な使い方](#基本的な使い方)
4. [DI（VContainer）との連携](#divcontainerとの連携)
5. [UDP通信の実装](#udp通信の実装)
6. [TCP通信の実装](#tcp通信の実装)
7. [アドレス指定機能](#アドレス指定機能)
8. [エラーハンドリング](#エラーハンドリング)
9. [Fluent API](#fluent-api)
10. [UDP/TCP切り替え機能](#udptcp切り替え機能)
11. [高度な使い方](#高度な使い方)
12. [トラブルシューティング](#トラブルシューティング)

## 概要

魔改造版 MessagePipe.Interprocessは、オリジナルのMessagePipe.Interprocessを拡張し、より柔軟で堅牢なプロセス間通信を実現するライブラリです。主な特徴は以下の通りです：

- **UDP/TCP両方のサポート**: 用途に応じて適切なプロトコルを選択可能
- **アドレス指定機能**: メッセージごとに送信先を動的に指定可能
- **エラーハンドリングの強化**: 接続エラーや送信エラーを柔軟に処理
- **Fluent API**: 直感的で読みやすいインターフェース
- **後方互換性**: 既存のコードを変更せずに利用可能

## UDP通信とTCP通信の違い

### UDP通信
- **特徴**: コネクションレス、軽量、高速だが信頼性は低い
- **適した用途**: リアルタイム性が重要で、多少のパケットロスが許容される場合（状態更新、位置情報など）
- **ブロードキャスト**: 同一ネットワーク内の複数デバイスに一斉送信可能

### TCP通信
- **特徴**: コネクション型、信頼性が高いが若干のオーバーヘッドあり
- **適した用途**: データの確実な送達が必要な場合（重要なコマンド、設定変更など）
- **接続管理**: 接続の確立・維持・切断の管理が必要

## 基本的な使い方

### 1. インストール

Unity の manifest.json に下記を追記します

```json
    "jp.megamin.messagepipe.interprocess": "https://github.com/medamap/MessagePipe.git?path=src/MessagePipe.Interprocess#1.8.1-extended.14",
```

### 2. 基本的な初期化（UDP）

```csharp
// MessagePipeの基本設定
// VContainer.Unity.LifetimeScope の Configure(IContainerBuilder builder) から受け取った builder を使う
var options = builder.RegisterMessagePipe();

// 既定のカスタムフォーマッター群
// Android IL2CPP では独自のデータの送受信はカスタムフォーマッターが必要です
var fixedFormatters = new List<IMessagePackFormatter>()
{
    // ここにカスタムフォーマッターを入れるてください
    // そうしないと、下のメッセージブローカーで登録しても
    // 送受信のタイミングで例外が出ます(特にAndroid IL2CPP)
    // Mono でビルドすれば、不要だった気がする
    new MyMessageFormatter(),
};

IFormatterResolver resolver = CompositeResolver.Create(
    fixedFormatters.ToArray(),
    new IFormatterResolver[] { ContractlessStandardResolver.Instance }
);

 // MessagePackSerializerOptions を設定及び、デフォルトとして設定する
 MessagePackSerializerOptions serializerOptions = MessagePackSerializerOptions.Standard.WithResolver(resolver);
 MessagePackSerializer.DefaultOptions = serializerOptions;

// UDP メッセージ送受信用のオプションを設定
MessagePipeInterprocessOptions udpOptions = builder.ToMessagePipeBuilder()
    .AddUdpInterprocessWithSubnet(
        "192.168.1.255",   // ブロードキャストアドレス
        12345,             // ポート
        // 正しいブロードキャストを行うための判定に使われる情報です
        "255.255.255.0",   // サブネットマスク
        "192.168.1.0",     // ネットワークアドレス
        // 一般的なネットワークでは下記でOK、特殊なネットワークでは依存注入の時点で例外が出る可能性があります
        false,             // バインドエラーを無視しない
        false,             // 送信エラーを無視しない
        opt => {
            opt.MessagePackSerializerOptions = serializerOptions;
        });

// メッセージブローカー登録
builder.ToMessagePipeBuilder().RegisterUpdInterprocessMessageBroker<string, MyMessage>(udpOptions);
```

### 3. 基本的な初期化（TCP）

```csharp
// MessagePipeの基本設定
var options = builder.RegisterMessagePipe();

// TCP メッセージ送受信用のオプションを設定
MessagePipeInterprocessTcpExtendedOptions tcpOptions =
    new MessagePipeInterprocessTcpExtendedOptions(
        "192.168.1.100", // 送信先IPアドレス
        12345)           // ポート
    {
        IgnoreConnectErrors = false, // 接続エラーを無視しない
        IgnoreSendErrors = false,    // 送信エラーを無視しない
        MaxRetryCount = 3,           // 最大リトライ回数
        ConnectionTimeoutMs = 5000,  // 接続タイムアウト(ms)
        SendTimeoutMs = 3000,        // 送信タイムアウト(ms)
        MessagePackSerializerOptions = serializerOptions // シリアライザオプション
    };

// メッセージブローカー登録
builder.ToMessagePipeBuilder().RegisterTcpInterprocessMessageBroker<string, MyMessage>(tcpOptions);
```

### 4. 基本的な送受信

```csharp
// 送信
await publisher.PublishAsync("topic", new MyMessage { Content =  Hello  });

// 受信
await subscriber.SubscribeAsync("topic", message => {
    Debug.Log($"Received: {message.Content}");
});
```

## DI（VContainer）との連携

魔改造版でも、MessagePipeの基本的なDIの仕組みは変わっていません。VContainerを使った依存性注入は引き続き同じように行えます。

### 1. 基本的なDIの流れ

- VContainerでサービスを登録
- コンストラクタインジェクションでパブリッシャー/サブスクライバーを取得
- 通常のMessagePipeと同じインターフェースを使用

### 2. 拡張機能へのアクセス方法

- 基本インターフェース（`IDistributedPublisher<TKey, TMessage>`）でDI
- 拡張機能を使う場合は具象クラス（`TcpDistributedPublisher<TKey, TMessage>`または`UdpDistributedPublisher<TKey, TMessage>`）にキャスト

### 3. VContainerでの登録例

```csharp
public void Configure(IContainerBuilder builder)
{
    // MessagePipeの基本設定
    var options = builder.RegisterMessagePipe();

    // UDP通信の登録
    var udpOptions = builder.ToMessagePipeBuilder()
        .AddUdpInterprocessWithSubnet(
            "255.255.255.255", // ブロードキャストアドレス
            12345,             // ポート
            "255.255.255.0",   // サブネットマスク
            "192.168.1.0",     // ネットワークアドレス
            false,             // バインドエラーを無視しない
            false,             // 送信エラーを無視しない
            opt => {
                opt.MessagePackSerializerOptions = serializerOptions;
            });
    
    builder.ToMessagePipeBuilder().RegisterUpdInterprocessMessageBroker<string, StatusMessage>(udpOptions);
    
    // TCP通信の登録
    var tcpOptions = new MessagePipeInterprocessTcpExtendedOptions("192.168.1.100", 12345)
    {
        IgnoreConnectErrors = true,
        IgnoreSendErrors = true,
        MaxRetryCount = 3
    };
    
    builder.ToMessagePipeBuilder().RegisterTcpInterprocessMessageBroker<string, CommandMessage>(tcpOptions);
    
    // サービスの登録
    builder.Register<NetworkService>(Lifetime.Singleton);
}
```

### 4. サービスでのDI例

```csharp
public class NetworkService
{
    private readonly IDistributedPublisher<string, StatusMessage> _statusPublisher;
    private readonly UdpDistributedPublisher<string, StatusMessage> _udpStatusPublisher;

    private readonly IDistributedPublisher<string, CommandMessage> _commandPublisher;
    private readonly TcpDistributedPublisher<string, CommandMessage> _tcpCommandPublisher;
    
    // 通常のDIパターン - インターフェースで注入
    public NetworkService(
        IDistributedPublisher<string, StatusMessage> statusPublisher,
        IDistributedPublisher<string, CommandMessage> commandPublisher)
    {
        _statusPublisher = statusPublisher;
        _udpStatusPublisher = statusPublisher as UdpDistributedPublisher<string, StatusMessage>;
        
        _commandPublisher = commandPublisher;
        _tcpCommandPublisher = commandPublisher as TcpDistributedPublisher<string, CommandMessage>;
    }
    
    // 基本的な送信（通常のMessagePipeと同じ）
    public async UniTask SendStatus(StatusMessage message)
    {
        await _statusPublisher.PublishAsync("status", message);
    }
    
    // UDP拡張機能を使った送信
    public async UniTask SendStatusToTarget(StatusMessage message, string targetAddress)
    {
        if (_udpStatusPublisher != null)
        {
            await _udpStatusPublisher.PublishToTargetAsync("status", message, targetAddress);
        }
        else
        {
            await _statusPublisher.PublishAsync("status", message);
        }
    }
    
    // TCP拡張機能を使った送信
    public async UniTask SendCommandWithRetry(CommandMessage message, string targetAddress)
    {
        if (_tcpCommandPublisher != null)
        {
            await _tcpCommandPublisher.CreatePublisher()
                .WithTarget(targetAddress)
                .WithRetry(3)
                .WithTimeout(TimeSpan.FromSeconds(2))
                .PublishAsync("command", message);
        }
        else
        {
            await _commandPublisher.PublishAsync("command", message);
        }
    }
}
```

## UDP通信の実装

### 1. UDP通信の初期化

```csharp
// UDP メッセージ送受信用のオプションを設定
MessagePipeInterprocessOptions udpOptions = builder.ToMessagePipeBuilder()
    .AddUdpInterprocessWithSubnet(
        "255.255.255.255", // ブロードキャストアドレス
        12345,             // ポート
        "255.255.255.0",   // サブネットマスク
        "192.168.1.0",     // ネットワークアドレス
        false,             // バインドエラーを無視しない
        false,             // 送信エラーを無視しない
        opt => {
            opt.MessagePackSerializerOptions = serializerOptions;
        });

// メッセージブローカー登録
builder.ToMessagePipeBuilder().RegisterUpdInterprocessMessageBroker<string, StatusMessage>(udpOptions);
```

### 2. UDP送信の実装例

```csharp
// 基本的な送信
await _publisher.PublishAsync( status , new StatusMessage {
    DeviceId =  device1 ,
    Status =  online  
});

// 送信先を指定した送信
await _udpPublisher.PublishToTargetAsync( status , new StatusMessage {
    DeviceId =  device1 ,
    Status =  online  
    }, "192.168.1.100");

// Fluent APIを使用した送信
await _udpPublisher.CreatePublisher()
    .WithTarget("192.168.1.100")
    .WithErrorCallback(ex => {
        Debug.LogError($"送信エラー: {ex.Message}");
    })
    .PublishAsync("status", new StatusMessage {
        DeviceId =  device1 ,
        Status =  online  
    });
```

### 3. UDP受信の実装例

```csharp
// 基本的な受信
await _subscriber.SubscribeAsync("status", message => {
    Debug.Log($"ステータス受信: デバイス={message.DeviceId}, 状態={message.Status}");
});

// フィルタ付き受信（特定の条件を満たすメッセージのみ処理）
await _subscriber.SubscribeAsync(
    "status",
    message => {
        Debug.Log($"ステータス受信: デバイス={message.DeviceId}, 状態={message.Status}");
    },
    message => message.DeviceId == "device1" // フィルタ条件
);
```

## TCP通信の実装

### 1. TCP通信の初期化

```csharp
// TCP メッセージ送受信用のオプションを設定
MessagePipeInterprocessTcpExtendedOptions tcpOptions =
    new MessagePipeInterprocessTcpExtendedOptions(
        "192.168.1.100", // 送信先IPアドレス
        12345)           // ポート
    {
        IgnoreConnectErrors = false, // 接続エラーを無視しない
        IgnoreSendErrors = false,    // 送信エラーを無視しない
        MaxRetryCount = 3,           // 最大リトライ回数
        ConnectionTimeoutMs = 5000,  // 接続タイムアウト(ms)
        SendTimeoutMs = 3000,        // 送信タイムアウト(ms)
        MessagePackSerializerOptions = serializerOptions // シリアライザオプション
    };

// メッセージブローカー登録
builder.ToMessagePipeBuilder().RegisterTcpInterprocessMessageBroker<string, CommandMessage>(tcpOptions);
```

### 2. TCP送信の実装例

```csharp
// 基本的な送信
await _publisher.PublishAsync("command", new CommandMessage {
    CommandId = "cmd1",
    Action = "start"
});

// 送信先を指定した送信
await _tcpPublisher.PublishToTargetAsync("command", new CommandMessage {
CommandId = "cmd1",
Action = "start"
}, "192.168.1.100");

// Fluent APIを使用した送信（リトライ・タイムアウト付き）
await _tcpPublisher.CreatePublisher()
    .WithTarget("192.168.1.100")
    .WithRetry(3)
    .WithTimeout(TimeSpan.FromSeconds(2))
    .WithErrorCallback(ex => {
        Debug.LogError($"コマンド送信エラー: {ex.Message}");
    })
    .WaitForCompletion()
    .PublishAsync("command", new CommandMessage {
        CommandId = "cmd1",
        Action = "start"
    });
```

### 3. TCP受信の実装例

```csharp
// 基本的な受信
await _subscriber.SubscribeAsync("command", message => {
    Debug.Log($"コマンド受信: ID={message.CommandId}, アクション={message.Action}");
});

// 非同期ハンドラを使用した受信
await _subscriber.SubscribeAsync(
    command ,
    async message => {
        Debug.Log($"コマンド受信: ID={message.CommandId}, アクション={message.Action}");
        await ProcessCommandAsync(message);
    }
);
```

## アドレス指定機能

メッセージごとに送信先を動的に指定するには、以下の2つの方法があります：

### 1. PublishToTargetAsyncメソッドを使用

```csharp
// 送信先アドレスを直接指定
await _publisher.PublishToTargetAsync("topic", message, "192.168.1.100");

// 送信先アドレスとポートを指定
await _publisher.PublishToTargetAsync("topic", message, "192.168.1.100", 12345);
```

### 2. IToAddressableインターフェースを実装

メッセージクラスに`IToAddressable`インターフェースを実装することで、メッセージ自身が送信先を持つようになります：

```csharp
[MessagePackObject]
public class DirectedMessage : IToAddressable
{
    [Key(0)] public string Content { get; set; }
    [Key(1)] public string FromAddress { get; set; }
    [Key(2)] public string ToAddress { get; set; }

    // IToAddressableインターフェースの実装
    public string GetToAddress() => ToAddress;
}

// 使用例
var message = new DirectedMessage {
    Content = "Hello",
    FromAddress = "192.168.1.10",
    ToAddress = "192.168.1.100"
};

// 通常の送信メソッドでも、IToAddressableが実装されていれば
// 自動的にToAddressに送信される
await _publisher.PublishAsync("topic", message);
```

### 3. IToPortableインターフェースを実装

ポート番号も動的に指定したい場合は、`IToPortable`インターフェースも実装します：

```csharp
[MessagePackObject]
public class FullyDirectedMessage : IToAddressable, IToPortable
{
    [Key(0)] public string Content { get; set; }
    [Key(1)] public string FromAddress { get; set; }
    [Key(2)] public string ToAddress { get; set; }
    [Key(3)] public int ToPort { get; set; }

    // IToAddressableインターフェースの実装
    public string GetToAddress() => ToAddress;
    
    // IToPortableインターフェースの実装
    public int GetPort() => ToPort;
}
```

## エラーハンドリング

魔改造版では、エラーハンドリングが大幅に強化されています。

### 1. 初期化時のエラーハンドリングオプション

```csharp
// UDP通信のエラーハンドリングオプション
var udpOptions = builder.ToMessagePipeBuilder()
    .AddUdpInterprocessWithSubnet(
        "255.255.255.255",
        12345,
        "255.255.255.0",
        "192.168.1.0",
        true,  // バインドエラーを無視する
        true,  // 送信エラーを無視する
        opt => {
            opt.MessagePackSerializerOptions = serializerOptions;
        });

// TCP通信のエラーハンドリングオプション
var tcpOptions = new MessagePipeInterprocessTcpExtendedOptions("192.168.1.100", 12345)
{
    IgnoreConnectErrors = true,  // 接続エラーを無視する
    IgnoreSendErrors = true,     // 送信エラーを無視する
    MaxRetryCount = 3,           // 最大リトライ回数
    RetryIntervalMs = 500        // リトライ間隔(ms)
};
```

### 2. 送信時のエラーハンドリング

```csharp
try
{
    await _publisher.PublishAsync("topic", message);
}
catch (Exception ex)
{
    Debug.LogError($"送信エラー: {ex.Message}");
    // エラー処理
}
```

### 3. Fluent APIでのエラーハンドリング

```csharp
await _publisher.CreatePublisher()
    .WithTarget("192.168.1.100")
    .WithRetry(3)
    .WithErrorCallback(ex => {
        Debug.LogError($"送信エラー: {ex.Message}");
    // エラー処理
    })
    .PublishAsync( topic , message);
```

## Fluent API

Fluent APIを使用すると、メソッドチェーンで直感的に送信オプションを設定できます。

### 1. TCP用Fluent API

```csharp
await _tcpPublisher.CreatePublisher()
    .WithTarget("192.168.1.100")       // 送信先アドレス
    .WithPort(12345)                   // 送信先ポート
    .WithRetry(3)                      // リトライ回数
    .WithRetryInterval(TimeSpan.FromMilliseconds(500)) // リトライ間隔
    .WithTimeout(TimeSpan.FromSeconds(2))  // タイムアウト
    .WithErrorCallback(ex => {         // エラーコールバック
        Debug.LogError($"送信エラー: {ex.Message}");
    })
    .WithCompletionCallback(() => {    // 完了コールバック
        Debug.Log("送信完了");
    })
    .WaitForCompletion()               // 完了を待機
    .PublishAsync( topic , message);   // 送信実行
```

### 2. UDP用Fluent API

```csharp
await _udpPublisher.CreatePublisher()
    .WithTarget("192.168.1.100")       // 送信先アドレス
    .WithPort(12345)                   // 送信先ポート
    .WithErrorCallback(ex => {         // エラーコールバック
        Debug.LogError($"送信エラー: {ex.Message}");
    })
    .PublishAsync("topic", message);   // 送信実行
```

## UDP/TCP切り替え機能

設定に基づいて動的にUDPとTCPを切り替える機能を実装できます：

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
                opt => {
                    opt.MessagePackSerializerOptions = serializerOptions;
                }
            );
    }
}

// 使用例
var options = CreateMessagingOptions(networkSettings, serializerOptions);

// 共通のメッセージブローカー登録メソッド
RegisterMessagingBrokers(builder, options);
```

### 共通のメッセージブローカー登録メソッド

```csharp
// プロトコルに応じて適切なブローカーを登録
public static void RegisterMessagingBrokers(IContainerBuilder builder, MessagePipeInterprocessOptions options)
{
    // TCP版を使用
    if (options is MessagePipeInterprocessTcpExtendedOptions ||
        options is MessagePipeInterprocessTcpUdsExtendedOptions)
    {
        builder.ToMessagePipeBuilder().RegisterTcpInterprocessMessageBroker<string, StatusMessage>(options);
        builder.ToMessagePipeBuilder().RegisterTcpInterprocessMessageBroker<string, CommandMessage>(options);
        // 他のメッセージタイプも同様に...
    }
    // 既存のUDP版を使用
    else
    {
        builder.ToMessagePipeBuilder().RegisterUpdInterprocessMessageBroker<string, StatusMessage>(options);
        builder.ToMessagePipeBuilder().RegisterUpdInterprocessMessageBroker<string, CommandMessage>(options);
        // 他のメッセージタイプも同様に...
    }
}
```

## 高度な使い方

### 1. 複数のプロトコルを同時に使用

異なるメッセージタイプに対して異なるプロトコルを使用することができます：

```csharp
// UDP通信の設定（ステータス更新用）
var udpOptions = builder.ToMessagePipeBuilder()
    .AddUdpInterprocessWithSubnet(
        "255.255.255.255",
        12345,
        "255.255.255.0",
        "192.168.1.0",
        true,
        true,
        opt => {
            opt.MessagePackSerializerOptions = serializerOptions;
        }
    );

// TCP通信の設定（コマンド送信用）
var tcpOptions = new MessagePipeInterprocessTcpExtendedOptions(
    "192.168.1.100",
    12346)
    {
        IgnoreConnectErrors = true,
        IgnoreSendErrors = true,
        MaxRetryCount = 3
    };

// それぞれのメッセージタイプに適したプロトコルを登録
builder.ToMessagePipeBuilder().RegisterUpdInterprocessMessageBroker<string, StatusMessage>(udpOptions);
builder.ToMessagePipeBuilder().RegisterTcpInterprocessMessageBroker<string, CommandMessage>(tcpOptions);
```

### 2. カスタムフィルタの実装

特定の条件を満たすメッセージのみを処理するカスタムフィルタを実装できます：

```csharp
// 自分宛のメッセージのみを処理するフィルタ
public class AddressFilter : MessageHandlerFilter<DirectedMessage>
{
private readonly string localAddress;

    public AddressFilter(string localAddress)
    {
        this.localAddress = localAddress;
    }
    
    public override bool Predicate(DirectedMessage message)
    {
        // ToAddressが自分のアドレスと一致するか、ブロードキャストアドレスの場合のみtrue
        return message.ToAddress == localAddress || 
               message.ToAddress == "255.255.255.255";
    }
}

// フィルタの使用
var addressFilter = new AddressFilter(localIpAddress);
await subscriber.SubscribeAsync(
    "topic",
    message => {
        Debug.Log($"メッセージ受信: {message.Content}");
    },
    new[] { addressFilter }
);
```

### 3. 非同期ハンドラとキャンセレーション

非同期処理とキャンセレーションを組み合わせた高度な受信処理：

```csharp
// キャンセレーショントークンソース
var cts = new CancellationTokenSource();

// 非同期ハンドラを使用した受信
await subscriber.SubscribeAsync(
    "command",
    async (message, ct) => {
        Debug.Log($"コマンド受信開始: {message.CommandId}");

        try {
            // 時間のかかる処理
            await ProcessCommandAsync(message, ct);
            Debug.Log($"コマンド処理完了: {message.CommandId}");
        }
        catch (OperationCanceledException) {
            Debug.Log($"コマンド処理がキャンセルされました: {message.CommandId}");
        }
    },
    cts.Token
);

// 後でキャンセルする場合
cts.Cancel();
```

## トラブルシューティング

### 1. 通信が確立されない場合

- **ファイアウォール設定**: 使用するポートがファイアウォールで許可されているか確認
- **IPアドレス**: 正しいIPアドレスを指定しているか確認
- **ポート番号**: 正しいポート番号を指定しているか確認
- **ネットワーク環境**: VPNや特殊なネットワーク設定がないか確認

### 2. メッセージが届かない場合

- **シリアライザ設定**: 送信側と受信側で同じMessagePackシリアライザオプションを使用しているか確認
- **トピック名**: 送信側と受信側で同じトピック名を使用しているか確認
- **メッセージ型**: 送信側と受信側で同じメッセージ型を使用しているか確認
- **アドレス指定**: `IToAddressable`を使用している場合、正しいアドレスが設定されているか確認

### 3. エラーハンドリング関連の問題

- **IgnoreErrors設定**: 適切なエラー無視設定になっているか確認
- **エラーコールバック**: エラーコールバックが正しく設定されているか確認
- **リトライ設定**: リトライ回数や間隔が適切か確認

### 4. DI関連の問題

- **登録順序**: MessagePipeの登録が他の依存関係より先に行われているか確認
- **スコープ**: 適切なライフタイムスコープで登録されているか確認
- **型の一致**: インターフェースと実装クラスの型パラメータが一致しているか確認

### 5. パフォーマンス問題

- **メッセージサイズ**: 大きすぎるメッセージを送信していないか確認
- **送信頻度**: 過度に高頻度で送信していないか確認
- **接続プール**: TCP接続プールの設定が適切か確認
