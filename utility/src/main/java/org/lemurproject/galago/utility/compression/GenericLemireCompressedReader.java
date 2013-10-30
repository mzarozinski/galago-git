/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import me.lemire.integercompression.BinaryPacking;
import me.lemire.integercompression.Composition;
import me.lemire.integercompression.IntWrapper;
import me.lemire.integercompression.IntegerCODEC;
import me.lemire.integercompression.VariableByte;

/**
 *
 * @author sjh
 */
public class GenericLemireCompressedReader implements CompressedLongReader {

  DataInputStream stream;
  int buffersize;
  int[] bufferIn;
  int blockCount = 0;
  int blockSize;
  int blockPos;
  int[] bufferOut;
  IntegerCODEC codec;
  IntWrapper inpos, outpos;

  public GenericLemireCompressedReader(InputStream stream) throws IOException {
    this(stream, new Composition(new BinaryPacking(), new VariableByte()));
  }

  public GenericLemireCompressedReader(InputStream stream, IntegerCODEC c) throws IOException {
    this.stream = new DataInputStream(stream);

    buffersize = this.stream.readInt();

    codec = c;

    blockPos = 0;
    blockSize = 0;

    bufferIn = new int[4 * buffersize + 1024];
    bufferOut = new int[4 * buffersize + 1024];

    inpos = new IntWrapper(0);
    outpos = new IntWrapper(0);

  }

  @Override
  public int readInt() throws IOException {
    if (blockPos >= outpos.get()) {
      blockPos = 0;
      decompressBlock();
    }

    int v = bufferOut[blockPos];
    blockPos++;
    return v;
  }

  @Override
  public long readLong() throws IOException {
    return 0;
  }

  @Override
  public void reset() throws IOException {
    blockPos = blockSize = 0;
    stream.reset();
  }

  private void decompressBlock() throws IOException {

    blockSize = stream.readInt();
    for (int i = 0; i < blockSize; i++) {
      bufferIn[i] = stream.readInt();
    }

    inpos.set(0);
    outpos.set(0);

    codec.uncompress(bufferIn, inpos, blockSize, bufferOut, outpos);

//    System.err.println("Block-" + blockCount + "[" + blockSize + " --> " + outpos.get() + "]");
    blockCount++;
  }
}
