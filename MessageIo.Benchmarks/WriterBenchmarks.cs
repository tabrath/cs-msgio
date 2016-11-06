using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Attributes.Jobs;

namespace MessageIo.Benchmarks
{
    [LegacyJitX86Job, RyuJitX64Job]
    public class WriterBenchmarks
    {
        private MessageWriter _writer;
        private byte[] _message;

        [Setup]
        public void Setup()
        {
            _message = new byte[1024];
            new Random(Environment.TickCount).NextBytes(_message);
            _writer = new MessageWriter(new MemoryStream(), false);
        }

        [Cleanup]
        public void Cleanup() => _writer?.Dispose();

        [Benchmark(Baseline = true)]
        public void WriteMessage() => _writer.WriteMessage(_message);

        [Benchmark]
        public Task WriteMessageAsync() => _writer.WriteMessageAsync(_message, CancellationToken.None);
    }
}
