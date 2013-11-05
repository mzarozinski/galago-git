/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression.integer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;

/**
 *
 * @author sjh
 */
public class GenericLemireCompressedWriter implements CompressedLongWriter {

  DataOutputStream output;
  long bytesWritten = 0;

  int buffersize;
  int bufferpos;
  int[] bufferIn;
  int[] bufferOut;
  int[] bufferVerify;
  IntegerCODEC codec;
  IntWrapper inpos, outpos, verifypos;
  int blockCount = 0;

  public GenericLemireCompressedWriter(OutputStream stream, IntegerCODEC c) throws IOException {
    output = new DataOutputStream(stream);

    codec = c;

    buffersize = 512;

    bufferpos = 0;
    bufferIn = new int[buffersize];
    bufferOut = new int[4 * buffersize + 1024];

    inpos = new IntWrapper(0);
    outpos = new IntWrapper(0);

    // write the block size, (and compression mechanism ?)
    output.writeInt(buffersize);
    bytesWritten += 4;
  }

  @Override
  public void writeInt(int value) throws IOException {

    bufferIn[bufferpos] = value;
    bufferpos++;

    if (bufferpos == buffersize) {
      compressFlush();
    }
  }

  @Override
  public void writeLong(long value) throws IOException {
    int nonFinalInt = 0x80000000;

    // need to separate longs into two ints, and compress separately
    int valuePart = (int) (value & 0x7FFFFFFF);
    value = value >>> 31;

    while (value != 0) {
      // write the last part, top bit is set to 1 -- indicates another part to reader.
      valuePart = nonFinalInt | valuePart;
      writeInt(valuePart);

      // prepare the next part
      valuePart = (int) (value & 0x7FFFFFFF);
      value = value >>> 31;
    }
    // write the final int (top bit is set to 0).
    writeInt(valuePart);

  }

  private void compressFlush() throws IOException {

    if (bufferpos == 0) {
      // nothing to write //
      return;
    }

    inpos.set(0);
    outpos.set(0);

    codec.compress(bufferIn, inpos, bufferpos, bufferOut, outpos);

    blockCount += 1;

    // clear bufferIn //
    bufferpos = 0;

    // determine the number of bytes in the final int:
    int intCount = outpos.get() - 1;
    int finalIntBytes = 0;

    // check if there is any information in the top 24 bits
    if ((bufferOut[outpos.get() - 1] & 0xFFFFFF00) == 0) {
      finalIntBytes = 1;
    } else if ((bufferOut[outpos.get() - 1] & 0xFFFF0000) == 0) {
      finalIntBytes = 2;
    } else if ((bufferOut[outpos.get() - 1] & 0xFF000000) == 0) {
      finalIntBytes = 3;
    } else {
      finalIntBytes = 4;
    }

    // two bytes for the size
    assert ((4 * intCount + finalIntBytes) < (1 << 16)) : "FAILED TO WRITE THE BLOCK HEADER : using a short to write " + (4 * intCount + finalIntBytes);

    int blockSize = 4 * intCount + finalIntBytes;
    output.writeShort(blockSize);

    bytesWritten += 2;

    for (int i = 0; i < (outpos.get() - 1); i++) {
      output.writeInt(bufferOut[i]);
      bytesWritten += 4;
    }

    if (finalIntBytes == 1) {
      output.writeByte(bufferOut[outpos.get() - 1]);
      bytesWritten += 1;

    } else if (finalIntBytes == 2) {
      output.writeShort(bufferOut[outpos.get() - 1]);
      bytesWritten += 2;

    } else if (finalIntBytes == 3) {
      output.writeShort(bufferOut[outpos.get() - 1]);
      output.writeByte((bufferOut[outpos.get() - 1] >>> 16));
      bytesWritten += 3;

    } else {
      output.writeInt(bufferOut[outpos.get() - 1]);
      bytesWritten += 4;

    }
    output.flush();
  }

  @Override
  public void flush() throws IOException {
    compressFlush();
    output.flush();
  }

  @Override
  public void close() throws IOException {
    compressFlush();
    output.close();
  }

  @Override
  public long getUnderlyingStreamPosition() throws IOException {
    compressFlush();
    return this.bytesWritten;
  }
}
