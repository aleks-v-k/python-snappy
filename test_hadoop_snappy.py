#!/usr/bin/env python

import os
import random
import struct
from unittest import TestCase

import snappy.snappy_hadoop


class SnappyStreaming(TestCase):

    def test_random(self):
        for _ in range(100):
            compressor = snappy.snappy_hadoop.StreamCompressor()
            decompressor = snappy.snappy_hadoop.StreamDecompressor()
            data = b""
            compressed = b""
            for _ in range(random.randint(0, 3)):
                chunk = os.urandom(
                    random.randint(0, snappy.snappy_hadoop._CHUNK_MAX * 2)
                )
                data += chunk
                compressed += compressor.add_chunk(
                    chunk
                )

            upper_bound = random.choice(
                [256, snappy.snappy_hadoop._CHUNK_MAX * 2]
            )
            while compressed:
                size = random.randint(0, upper_bound)
                chunk, compressed = compressed[:size], compressed[size:]
                chunk = decompressor.decompress(chunk)
                self.assertEqual(data[:len(chunk)], chunk)
                data = data[len(chunk):]

            decompressor.flush()
            self.assertEqual(len(data), 0)

    def test_concatenation(self):
        data1 = os.urandom(snappy.snappy_hadoop._CHUNK_MAX * 2)
        data2 = os.urandom(4096)
        decompressor = snappy.snappy_hadoop.StreamDecompressor()
        self.assertEqual(
                decompressor.decompress(
                    snappy.snappy_hadoop.StreamCompressor().compress(data1) +
                    snappy.snappy_hadoop.StreamCompressor().compress(data2)),
                data1 + data2)


if __name__ == "__main__":
    import unittest
    unittest.main()
