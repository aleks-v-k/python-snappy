#!/usr/bin/env python
#
# Copyright (c) 2011, Andres Moreira <andres@andresmoreira.com>
#               2011, Felipe Cruz <felipecruz@loogica.net>
#               2012, JT Olds <jt@spacemonkey.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the authors nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL ANDRES MOREIRA BE LIABLE FOR ANY DIRECT,
# INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

"""The module implements compression/decompression with snappy using
Hadoop snappy format: https://github.com/kubo/snzip#hadoop-snappy-format

Expected usage like:

    import snappy

    src = 'uncompressed'
    dst = 'compressed'
    dst2 = 'decompressed'

    with open(src, 'rb') as fin, open(dst, 'wb') as fout:
        snappy.hadoop_stream_compress(src, dst)

    with open(dst, 'rb') as fin, open(dst2, 'wb') as fout:
        snappy.hadoop_stream_decompress(fin, fout)

    with open(src, 'rb') as fin1, open(dst2, 'rb') as fin2:
        assert fin1.read() == fin2.read()

"""

import sys
import struct

try:
    from ._snappy import UncompressError, compress, decompress, \
                         isValidCompressed, uncompress, _crc32c
except ImportError:
    from .snappy_cffi import UncompressError, compress, decompress, \
                             isValidCompressed, uncompress, _crc32c


SNAPPY_BUFFER_SIZE_DEFAULT = 256 * 1024


_compress = compress
_uncompress = uncompress


def hadoop_snappy_max_input_size(block_size):
    buffer_size = block_size if block_size else SNAPPY_BUFFER_SIZE_DEFAULT
    compression_overhead = (buffer_size / 6) + 32
    return buffer_size - compression_overhead


def prepare_num(num):
    big_endian_uint = struct.pack('>I', num)
    return big_endian_uint


def read_int(fin):
    data = fin.read(4)
    if not data:
        return 0
    return struct.unpack('>I', data)[0]


class StreamCompressor(object):

    """This class implements the compressor-side of the proposed Snappy framing
    format, found at

        http://code.google.com/p/snappy/source/browse/trunk/framing_format.txt
            ?spec=svn68&r=71

    This class matches the interface found for the zlib module's compression
    objects (see zlib.compressobj), but also provides some additions, such as
    the snappy framing format's ability to intersperse uncompressed data.

    Keep in mind that this compressor object does no buffering for you to
    appropriately size chunks. Every call to StreamCompressor.compress results
    in a unique call to the underlying snappy compression method.
    """


    def __init__(self):
        pass

    def add_chunk(self, data):
        """Add a chunk containing 'data', returning a string that is framed and
        (optionally, default) compressed. This data should be concatenated to
        the tail end of an existing Snappy stream. In the absence of any
        internal buffering, no data is left in any internal buffers, and so
        unlike zlib.compress, this method returns everything.

        If compress is None, compression is determined automatically based on
        snappy's performance. If compress == True, compression always happens,
        and if compress == False, compression never happens.
        """
        out = []
        uncompressed_length = len(data)
        out.append(prepare_num(uncompressed_length))
        compressed_chunk = _compress(data)
        compressed_length = len(compressed_chunk)
        out.append(prepare_num(compressed_length))
        out.append(compressed_chunk)
        return b"".join(out)

    def compress(self, data):
        """This method is simply an alias for compatibility with zlib
        compressobj's compress method.
        """
        return self.add_chunk(data)

    def flush(self, mode=None):
        """This method does nothing and only exists for compatibility with
        the zlib compressobj
        """
        pass

    def copy(self):
        """This method exists for compatibility with the zlib compressobj.
        """
        copy = StreamCompressor()
        copy._header_chunk_written = self._header_chunk_written
        return copy


def stream_compress(src, dst, blocksize=None):
    """Takes an incoming file-like object and an outgoing file-like object,
    reads data from src, compresses it, and writes it to dst. 'src' should
    support the read method, and 'dst' should support the write method.

    The default blocksize is good for almost every scenario.
    """
    compressor = StreamCompressor()
    blocksize = hadoop_snappy_max_input_size(blocksize)
    while True:
        buf = src.read(blocksize)
        if not buf:
            break
        buf = compressor.add_chunk(buf)
        if buf:
            dst.write(buf)


def stream_decompress(src, dst):
    """Takes an incoming file-like object and an outgoing file-like object,
    reads data from src, decompresses it, and writes it to dst. 'src' should
    support the read method, and 'dst' should support the write method.

    The default blocksize is good for almost every scenario.
    """
    while True:
        uncompressed_length = read_int(src)
        if not uncompressed_length:
            break
        compressed_length = read_int(src)
        if not compressed_length:
            raise UncompressError("Failed to read compressed chunk length")
        compressed_chunk = src.read(compressed_length)
        if len(compressed_chunk) != compressed_length:
            raise UncompressError("Failed to read compressed chunk")
        uncompressed_chunk = _uncompress(compressed_chunk)
        if len(uncompressed_chunk) != uncompressed_length:
            raise UncompressError(
                "Specified uncompressed chunk length {} != "
                "real uncompressed chunk length {}".format(
                    uncompressed_length,
                    len(uncompressed_chunk)
                )
            )
        dst.write(uncompressed_chunk)
