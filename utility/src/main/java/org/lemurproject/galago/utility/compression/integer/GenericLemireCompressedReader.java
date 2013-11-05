/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression.integer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;

/**
 *
 * @author sjh
 */
public class GenericLemireCompressedReader implements CompressedLongReader {

  DataInputStream stream;
  long currentOffset;

  int buffersize;
  int[] bufferIn;
  int blockCount = 0;
  int byteCount;
  int blockPos;
  int[] bufferOut;
  IntegerCODEC codec;
  IntWrapper inpos, outpos;

  public GenericLemireCompressedReader(InputStream stream, IntegerCODEC c) throws IOException {
    this.stream = new DataInputStream(stream);

    buffersize = this.stream.readInt();
    currentOffset = 4;

    codec = c;

    blockPos = 0;
    byteCount = 0;

    bufferIn = new int[4 * buffersize + 1024];
    bufferOut = new int[4 * buffersize + 1024];

    inpos = new IntWrapper(0);
    outpos = new IntWrapper(0);

  }

  @Override
  public int readInt() throws IOException {
    while (blockPos >= outpos.get()) {
      blockPos = 0;
      decompressBlock();
    }

    int v = bufferOut[blockPos];
    blockPos++;
    return v;
  }

  @Override
  public long readLong() throws IOException {
    long value = 0;

    int part = readInt();
    value += (part & 0x7FFFFFFF);

    // if the top bit is set --
    int pos = 31;
    while ((part >>> 31) == 1) {
      part = readInt();

      value += ((long) (part & 0x7FFFFFFF)) << pos;
      pos += 31;
    }

    return value;
  }

  @Override
  public boolean seek(long absoluteOffset) throws IOException {
    outpos.set(0);
    blockPos = 0;

    long relativeOffset = (absoluteOffset - currentOffset);
    if (relativeOffset > 0) {
      // same as skip -- but actually works //
      while (currentOffset < absoluteOffset) {
        stream.read();
        currentOffset++;
      }
      return true;
    }
    return false;
  }

  private void decompressBlock() throws IOException {

    byteCount = stream.readUnsignedShort();
    currentOffset = currentOffset + 2;
    
    int bufferInLength = 0;

    while (byteCount >= 4) {
      bufferIn[bufferInLength] = stream.readInt();
      currentOffset += 4;

      bufferInLength += 1;
      byteCount -= 4;
    }

    if (byteCount == 1) {
      int finalInt = stream.readUnsignedByte();
      currentOffset += 1;

      bufferIn[bufferInLength] = finalInt;
      bufferInLength += 1;

    } else if (byteCount == 2) {
      int finalInt = stream.readUnsignedShort();
      currentOffset += 2;

      bufferIn[bufferInLength] = finalInt;
      bufferInLength += 1;

    } else if (byteCount == 3) {
      int finalInt = stream.readUnsignedShort();
      finalInt += stream.readUnsignedByte() << 16;
      currentOffset += 3;

      bufferIn[bufferInLength] = finalInt;
      bufferInLength += 1;
    }

    inpos.set(0);
    outpos.set(0);

    codec.uncompress(bufferIn, inpos, bufferInLength, bufferOut, outpos);

//    System.err.println("Block-" + blockCount + "[" + byteCount + " --> " + outpos.get() + "] " + currentOffset);
    blockCount++;
  }
}
