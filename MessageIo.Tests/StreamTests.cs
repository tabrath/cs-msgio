using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace MessageIo.Tests
{
    [TestFixture]
    public class StreamTests
    {
        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
        public void TestReadWrite(LengthPrefixStyle lps)
        {
            using (var mem = new MemoryStream())
            using (var stream = new MessageStream(mem, true, lps))
            {
                SubtestReadWrite(stream, lps, () => stream?.Seek(0, SeekOrigin.Begin));
            }
        }

        private static void SubtestReadWrite(Stream stream, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[ReaderWriterTests.GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            msgs.ForEach(msg => stream.Write(msg, 0, msg.Length));

            reset();

            foreach (var msg in msgs)
            {
                var msg2 = new byte[msg.Length];
                stream.Read(msg2, 0, msg2.Length);

                Assert.That(msg2, Is.EqualTo(msg));
            }
        }

        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
        public async Task TestReadWriteAsync(LengthPrefixStyle lps)
        {
            using (var mem = new MemoryStream())
            using (var stream = new MessageStream(mem, true, lps))
            {
                await SubtestReadWriteAsync(stream, lps, () => stream?.Seek(0, SeekOrigin.Begin));
            }
        }

        private static async Task SubtestReadWriteAsync(Stream stream, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[ReaderWriterTests.GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            var tasks = msgs.Select(msg => stream.WriteAsync(msg, 0, msg.Length, CancellationToken.None));
            await Task.WhenAll(tasks);

            reset();

            foreach (var msg in msgs)
            {
                var msg2 = new byte[msg.Length];
                await stream.ReadAsync(msg2, 0, msg2.Length, CancellationToken.None);

                Assert.That(msg2, Is.EqualTo(msg));
            }
        }


        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
        public void TestReadWriteMessage(LengthPrefixStyle lps)
        {
            using (var mem = new MemoryStream())
            using (var stream = new MessageStream(mem, true, lps))
            {
                SubtestReadWriteMessage(stream, lps, () => stream?.Seek(0, SeekOrigin.Begin));
            }
        }

        private static void SubtestReadWriteMessage(MessageStream stream, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[ReaderWriterTests.GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            msgs.ForEach(stream.WriteMessage);

            reset();

            foreach (var msg in msgs)
            {
                var msg2 = stream.ReadMessage();

                Assert.That(msg2, Is.EqualTo(msg));
            }
        }

        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
        public async Task TestReadWriteMessageAsync(LengthPrefixStyle lps)
        {
            using (var mem = new MemoryStream())
            using (var stream = new MessageStream(mem, true, lps))
            {
                await SubtestReadWriteMessageAsync(stream, lps, () => stream?.Seek(0, SeekOrigin.Begin));
            }
        }

        private static async Task SubtestReadWriteMessageAsync(MessageStream stream, LengthPrefixStyle lps, Action reset)
        {
            var rand = new Random(Environment.TickCount);
            var msgs = Enumerable.Range(0, 64).Select(_ =>
            {
                var msg = new byte[ReaderWriterTests.GetMaxLength(lps, rand.Next(16, 1024))];
                rand.NextBytes(msg);
                return msg;
            }).ToList();

            var tasks = msgs.Select(msg => stream.WriteMessageAsync(msg, CancellationToken.None));
            await Task.WhenAll(tasks);

            reset();

            foreach (var msg in msgs)
            {
                var msg2 = await stream.ReadMessageAsync(CancellationToken.None);

                Assert.That(msg2, Is.EqualTo(msg));
            }
        }

        private static void MakeTcpStreamPair(out Stream aStream, out Stream bStream)
        {
            var listener = new TcpListener(IPAddress.Loopback, new Random(Environment.TickCount).Next(1024, 8192));
            listener.Start();
            var accept = listener.AcceptTcpClientAsync();
            var a = new TcpClient();
            a.ExclusiveAddressUse = false;
            a.NoDelay = true;
            a.Connect((IPEndPoint) listener.LocalEndpoint);
            var b = accept.Result;
            b.NoDelay = true;
            listener.Stop();

            aStream = a.GetStream();
            bStream = b.GetStream();
        }

        [TestCase(LengthPrefixStyle.Int8)]
        [TestCase(LengthPrefixStyle.UInt8)]
        [TestCase(LengthPrefixStyle.Int16)]
        [TestCase(LengthPrefixStyle.UInt16)]
        [TestCase(LengthPrefixStyle.Int32)]
        [TestCase(LengthPrefixStyle.UInt32)]
        [TestCase(LengthPrefixStyle.Int64)]
        [TestCase(LengthPrefixStyle.UInt64)]
        [TestCase(LengthPrefixStyle.Varint)]
        [TestCase(LengthPrefixStyle.UVarint)]
        public void CombineStreamPair_TestReadWriteMessage(LengthPrefixStyle lps)
        {
            Stream a = null, b = null;
            try
            {
                MakeTcpStreamPair(out a, out b);
                using (var stream = MessageStream.Combine(new MessageReader(a, true, lps), new MessageWriter(b, true, lps)))
                {
                    SubtestReadWriteMessage(stream, lps, () => {});
                }
            }
            finally
            {
                a?.Dispose();
                b?.Dispose();
            }
        }
    }
}
