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
    public class ReaderBenchmarks
    {
        private MessageReader _reader;
        private Stream _stream;

        [Setup]
        public void Setup()
        {
            _stream = new MemoryStream();
            var rand = new Random(Environment.TickCount);

            using (var writer = new MessageWriter(_stream, true))
            {
                for (var i = 0; i < 2; i++)
                {
                    var bytes = new byte[1024];
                    rand.NextBytes(bytes);
                    writer.WriteMessage(bytes);
                }
            }

            _reader = new MessageReader(_stream, true);
        }

        [Cleanup]
        public void Cleanup()
        {
            _reader?.Dispose();
        }

        [Benchmark(Baseline = true)]
        public byte[] ReadMessage()
        {
            _stream.Seek(0, SeekOrigin.Begin);
            return _reader.ReadMessage();
        }

        [Benchmark]
        public Task<byte[]> ReadMessageAsync()
        {
            _stream.Seek(0, SeekOrigin.Begin);
            return _reader.ReadMessageAsync(CancellationToken.None);
        }
    }
}
