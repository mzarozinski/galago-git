/*
 *  BSD License (http://lemurproject.org/galago-license)
 */

package org.lemurproject.galago.utility.compression;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 *
 * @author sjh
 */
public class GZIPWriter extends CompressedDataWriter {
  private final DataOutputStream output;

  public GZIPWriter(OutputStream stream) throws IOException{
    output = new DataOutputStream(new GZIPOutputStream(stream));
  }
  
  @Override
  public void writeBoolean(boolean bool) throws IOException {
    output.writeBoolean(bool);
  }

  @Override
  public void writeByte(byte b) throws IOException {
    output.writeByte(b);
  }

  @Override
  public void writeInt(int i) throws IOException {
    output.writeInt(i);
  }

  @Override
  public void writeLong(long l) throws IOException {
    output.writeLong(l);
  }

  @Override
  public void writeFloat(float f) throws IOException {
    output.writeFloat(f);
  }

  @Override
  public void writeDouble(double d) throws IOException {
    output.writeDouble(d);
  }

  @Override
  public void flush() throws IOException {
    output.flush();
  }

  @Override
  public void close() throws IOException {
    output.close();
  }
}
