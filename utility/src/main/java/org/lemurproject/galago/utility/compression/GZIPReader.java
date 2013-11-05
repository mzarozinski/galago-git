/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author sjh
 */
public class GZIPReader extends CompressedDataReader {

  private final DataInputStream input;

  public GZIPReader(InputStream stream) throws IOException {
    input = new DataInputStream(new GZIPInputStream(stream));
  }

  @Override
  public boolean readBoolean() throws IOException {
    return input.readBoolean();
  }

  @Override
  public byte readByte() throws IOException {
    return input.readByte();
  }

  @Override
  public int readInt() throws IOException {
    return input.readInt();
  }

  @Override
  public long readLong() throws IOException {
    return input.readLong();
  }

  @Override
  public float readFloat() throws IOException {
    return input.readFloat();
  }

  @Override
  public double readDouble() throws IOException {
    return input.readDouble();
  }

  @Override
  public void close() throws IOException {
    input.close();
  }
}
