""" Python 'gzip_codec' Codec - gzip compression encoding

    This is an alternative to the zlib_codec, that supports RFC1952
    (gzip) format files, as opposed to RFC1950 (zlib).

    License: same as Python's zlib_codec.py license (Python license)
"""
import codecs
import gzip
import os
import StringIO
import struct
import zlib  # this codec needs the optional zlib module !


# Codec APIs

def gzip_encode(input, errors='strict'):

    """ Encodes the object input and returns a tuple (output
        object, length consumed).

        errors defines the error handling to apply. It defaults to
        'strict' handling which is the only currently supported
        error handling for this codec.

    """
    assert errors == 'strict'
    outbuf = StringIO.StringIO()
    gzip.GzipFile(mode='wb', fileobj=outbuf).write(input)
    return (outbuf.getvalue(), len(input))


def gzip_decode(input, errors='strict'):

    """ Decodes the object input and returns a tuple (output
        object, length consumed).

        input must be an object which provides the bf_getreadbuf
        buffer slot. Python strings, buffer objects and memory
        mapped files are examples of objects providing this slot.

        errors defines the error handling to apply. It defaults to
        'strict' handling which is the only currently supported
        error handling for this codec.

    """
    assert errors == 'strict'
    inbuf = StringIO.StringIO(input)
    output = gzip.GzipFile(mode='rb', fileobj=inbuf).read()
    return (output, len(input))


class Codec(codecs.Codec):

    def encode(self, input, errors='strict'):
        return gzip_encode(input, errors)

    def decode(self, input, errors='strict'):
        return gzip_decode(input, errors)


class IncrementalEncoder(codecs.IncrementalEncoder):
    def __init__(self, errors='strict'):
        assert errors == 'strict'
        self.errors = errors
        self.reset()

    def encode(self, input, final=False):
        c = ''
        if not self.gzipobj:
            outbuf = StringIO.StringIO()
            self.gzipobj = gzip.GzipFile(mode='wb', fileobj=outbuf)
            c += outbuf.getvalue()
        self.crc = zlib.crc32(input, self.crc) & 0xffffffffL
        self.size += len(input)
        if final:
            c += self.compressobj.compress(input)
            c += self.compressobj.flush()
            c += struct.pack('I', self.crc)
            c += struct.pack('I', self.size & 0xffffffffL)
            return c
        else:
            return c + self.compressobj.compress(input)

    def reset(self):
        self.gzipobj = None
        self.crc = zlib.crc32("") & 0xffffffffL
        self.size = 0
        self.compressobj = zlib.compressobj(9,
                                            zlib.DEFLATED,
                                            -zlib.MAX_WBITS,
                                            zlib.DEF_MEM_LEVEL,
                                            0)


class IncrementalDecoder(codecs.IncrementalDecoder):
    """
    Incrementally decodes a *single*-member gzip stream.
    """
    def __init__(self, errors='strict'):
        assert errors == 'strict'
        self.errors = errors
        self.reset()

    def decode(self, input, final=False):
        if not self.gzipobj:
            if len(input) < 10:
                raise IOError("Cannot incrementally decode a partial gzip header.")
            inbuf = StringIO.StringIO(input)
            self.gzipobj = gzip.GzipFile(mode='rb', fileobj=inbuf)
            # The following copied/modified from gzip.py.
            self.gzipobj._init_read()
            self.gzipobj._read_gzip_header()
            self.gzipobj.decompress = zlib.decompressobj(-zlib.MAX_WBITS)
            # End copy/mod from gzip.py.
            # Continue and consume the rest of the starting increment.
            input = inbuf.read()

        if final:
            if len(input) < 8:
                raise IOError("Cannot incrementally decode a partial gzip footer.")
            uncompress = self.gzipobj.decompress.decompress(input)
            uncompress += self.gzipobj.decompress.flush()
            self.gzipobj.fileobj = StringIO.StringIO(input)
            self.gzipobj.fileobj.seek(0, os.SEEK_END)
            self.gzipobj._add_read_data(uncompress)
            self.gzipobj._read_eof()
            return uncompress
        else:
            uncompress = self.gzipobj.decompress.decompress(input)
            self.gzipobj._add_read_data(uncompress)
            return uncompress

    def reset(self):
        self.gzipobj = None


class StreamWriter(Codec, codecs.StreamWriter):
    pass


class StreamReader(Codec, codecs.StreamReader):
    pass


# encodings module API

def getregentry(name='gzip'):
    return codecs.CodecInfo(
        name=name,
        encode=gzip_encode,
        decode=gzip_decode,
        incrementalencoder=IncrementalEncoder,
        incrementaldecoder=IncrementalDecoder,
        streamreader=StreamReader,
        streamwriter=StreamWriter,
    )
