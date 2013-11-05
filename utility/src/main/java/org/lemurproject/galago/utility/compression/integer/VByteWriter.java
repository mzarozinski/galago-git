/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression.integer;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author sjh
 */
public class VByteWriter implements CompressedLongWriter {

  long bytesWritten = 0;
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
    // assert (value >= 0) : "VByteWriter can not compress negative values.";

    // if less than 7 bytes:
    if ((value | 0x7f) == 0x7f) {
      output.write((int) (value | 0x80));
      bytesWritten += 1;

    // otherwise more than 7 bytes:
    } else {

      output.write((int) (value & 0x7f));
      bytesWritten += 1;
      value >>>= 7;

      while ((value | 0x7f) != 0x7f) {
        output.write((int) (value & 0x7f));
        bytesWritten += 1;
        value >>= 7;
      }

      output.write((int) (value | 0x80));
      bytesWritten += 1;
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

  @Override
  public long getUnderlyingStreamPosition() {
    return this.bytesWritten;
  }
}
