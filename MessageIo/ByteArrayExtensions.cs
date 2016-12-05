using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MessageIo
{
    internal static class ByteArrayExtensions
    {
        public static byte[] Slice(this byte[] array, int offset, int? count = null)
        {
            var output = new byte[count ?? array.Length - offset];
            Buffer.BlockCopy(array, offset, output, 0, output.Length);
            return output;
        }
    }
}
