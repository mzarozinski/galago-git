/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import me.lemire.integercompression.BinaryPacking;
import me.lemire.integercompression.Composition;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;
import me.lemire.integercompression.VariableByte;

/**
 *
 * @author sjh
 */
public class GenericLemireCompressedWriter implements CompressedLongWriter {

  DataOutputStream output;
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
  }

  @Override
  public void writeInt(int value) throws IOException {
    assert (value >= 0) : "GenericCompressedIntWriter can not compress negative values.";

    bufferIn[bufferpos] = value;
    bufferpos++;

    if (bufferpos == buffersize) {
      compressFlush();
    }
  }

  @Override
  public void writeLong(long value) throws IOException {
    assert (value >= 0) : "GenericCompressedIntWriter can not compress negative values.";

    // need to separate longs into two ints, and compress separately
    // USER's problem to determine if
  }

  public void compressFlush() throws IOException {

    inpos.set(0);
    outpos.set(0);

    codec.compress(bufferIn, inpos, bufferpos, bufferOut, outpos);

    blockCount += 1;

    // clear bufferIn //
    bufferpos = 0;

    // check if we're done //
    if(outpos.get() == 0){
      return;
    }
    
    // determine the number of bytes in the final int:
    int intCount = outpos.get() - 1;
    int finalIntBytes = 4;
    if (bufferOut[outpos.get() - 1] < (1 << 8)) {
      finalIntBytes = 1;
    } else if (bufferOut[outpos.get() - 1] < (1 << 16)) {
      finalIntBytes = 2;
    } else if (bufferOut[outpos.get() - 1] < (1 << 24)) {
      finalIntBytes = 3;
    }
    
    // two bytes for the size
    output.writeInt(4 * intCount + finalIntBytes);
    for (int i = 0; i < (outpos.get() - 1); i++) {
      output.writeInt(bufferOut[i]);
    }
    
    if (finalIntBytes == 1) {
      output.writeByte(bufferOut[outpos.get() - 1]);
    
    } else if (finalIntBytes == 2) {
      output.writeShort(bufferOut[outpos.get() - 1]);

    } else if (finalIntBytes == 3) {
      // writing //
      output.writeShort(bufferOut[outpos.get() - 1]);
      output.writeByte((bufferOut[outpos.get() - 1] >>> 16));
     
    } else {
      output.writeInt(bufferOut[outpos.get() - 1]);
    }
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
}
