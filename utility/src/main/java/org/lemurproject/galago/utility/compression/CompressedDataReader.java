/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.IOException;
import org.lemurproject.galago.utility.Utility;

/**
 * Generic compressed data stream interface, supports each of the primitive data
 * items.
 *
 * @author sjh
 */
public abstract class CompressedDataReader {

  public abstract boolean readBoolean() throws IOException;

  public abstract byte readByte() throws IOException;

  public abstract int readInt() throws IOException;

  public abstract long readLong() throws IOException;

  public abstract float readFloat() throws IOException;

  public abstract double readDouble() throws IOException;

  public String readString() throws IOException {
    // UTF-8 implicit
    byte[] d = readByteArray();
    return Utility.toString(d);
  }

  public byte[] readByteArray() throws IOException {
    int len = readInt();
    byte[] d = new byte[len];
    for (int i = 0; i < len; i++) {
      d[i] = readByte();
    }
    return d;
  }

  public abstract void close() throws IOException;
}
