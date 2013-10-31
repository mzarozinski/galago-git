/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

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
    blockPos = byteCount = 0;
    stream.reset();
  }

  private void decompressBlock() throws IOException {
    
    byteCount = stream.readUnsignedShort();
    int bufferInLength = 0;

    while (byteCount >= 4) {
      bufferIn[bufferInLength] = stream.readInt();
      bufferInLength += 1;
      byteCount -= 4;
    }

    if (byteCount == 1) {
      int finalInt = stream.readUnsignedByte();
      bufferIn[bufferInLength] = finalInt;
      bufferInLength += 1;

    } else if(byteCount == 2){
      int finalInt = stream.readUnsignedShort();
      bufferIn[bufferInLength] = finalInt;
      bufferInLength += 1;      
      
    } else if(byteCount == 3){
      int finalInt = stream.readUnsignedShort();
      finalInt += stream.readUnsignedByte() << 16;
      bufferIn[bufferInLength] = finalInt;
      bufferInLength += 1;      
    }

    inpos.set(0);
    outpos.set(0);

    codec.uncompress(bufferIn, inpos, bufferInLength, bufferOut, outpos);

//    System.err.println("Block-" + blockCount + "[" + blockSize + " --> " + outpos.get() + "]");
    blockCount++;
  }
}
