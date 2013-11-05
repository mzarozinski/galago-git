/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.IOException;
import org.lemurproject.galago.utility.Utility;

/**
 * Generic compressed data stream, supports each of the primitive data items.
 *
 * @author sjh
 */
public abstract class CompressedDataWriter {

  public abstract void writeBoolean(boolean bool) throws IOException;

  public abstract void writeByte(byte b) throws IOException;

  public abstract void writeInt(int i) throws IOException;

  public abstract void writeLong(long l) throws IOException;

  public abstract void writeFloat(float f) throws IOException;

  public abstract void writeDouble(double d) throws IOException;

  public void writeString(String s) throws IOException {
    // implicitly uses UTF-8
    byte[] bytes = Utility.fromString(s);
    writeByteArray(bytes);
  }

  public void writeByteArray(byte[] ba) throws IOException {
    writeInt(ba.length);
    for (int i = 0; i < ba.length; i++) {
      writeByte(ba[i]);
    }
  }

  public abstract void flush() throws IOException;

  public abstract void close() throws IOException;
}
