using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Cysharp.Threading.Tasks;

namespace MessagePipe.Interprocess.Benchmark
{
    public class MyAsyncMessageHandler : IAsyncMessageHandler<byte[]>
    {
        public UniTask HandleAsync(byte[] message, CancellationToken cancellationToken)
        {
            return default(UniTask);
        }
    }
    public class MyAsyncHandler : IAsyncRequestHandler<int, byte[]>
    {
        public UniTask<byte[]> InvokeAsync(int request, CancellationToken cancellationToken = default)
        {
            if (request == -1)
            {
                throw new Exception("NO -1");
            }
            else
            {
                return new UniTask<byte[]>(new byte[request]);
            }
        }
    }
}
