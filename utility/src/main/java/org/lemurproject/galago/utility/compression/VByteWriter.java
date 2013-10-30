/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author sjh
 */
public class VByteWriter implements CompressedLongWriter {

  OutputStream output;

  public VByteWriter(OutputStream stream) {
    this.output = stream;
  }

  @Override
  public void writeInt(int value) throws IOException {
    writeLong((long) value);
  }

  @Override
  public void writeLong(long value) throws IOException {
    assert (value >= 0) : "VByteWriter can not compress negative values.";

    if (value < 1 << 7) {
      output.write((int) (value | 0x80));
    } else if (value < 1 << 14) {
      output.write((int) (value >> 0) & 0x7f);
      output.write((int) ((value >> 7) & 0x7f) | 0x80);
    } else if (value < 1 << 21) {
      output.write((int) (value >> 0) & 0x7f);
      output.write((int) (value >> 7) & 0x7f);
      output.write((int) ((value >> 14) & 0x7f) | 0x80);
    } else {
      while (value >= 1 << 7) {
        output.write((int) (value & 0x7f));
        value >>= 7;
      }

      output.write((int) (value | 0x80));
    }
  }

  @Override
  public void flush() throws IOException {
    // no memory here -- nothing specific to flush
    this.output.flush();
  }

  /**
   * ONLY USE THIS FUNCTION WHEN THE UNDERLYING STREAM SHOULD BE CLOSED.
   * 
   * @throws IOException 
   */
  @Override
  public void close() throws IOException {
    this.output.close();
  }
}
