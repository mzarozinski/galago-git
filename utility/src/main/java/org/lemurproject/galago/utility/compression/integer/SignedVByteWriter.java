/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression.integer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author sjh
 */
public class SignedVByteWriter implements CompressedLongWriter {

  long bytesWritten = 0;
  DataOutputStream output;

  public SignedVByteWriter(OutputStream stream) {
    this.output = new DataOutputStream(stream);
  }

  @Override
  public void writeInt(int value) throws IOException {
    writeLong((long) value);
  }

  @Override
  public void writeLong(long value) throws IOException {
    // assert (value >= 0) : "VByteWriter can not compress negative values.";

    long finalByteFlag = 0x80;
    if (value < 0) {
      finalByteFlag = 0xC0;
      value *= -1;
    }

    // check if the value uses 6 bits or fewer
    if ((value & (0x3f)) == value) {
      output.write((int) (value | finalByteFlag));
      bytesWritten += 1;

    } else {
      // otherwise write the final seven bits repeatedly
      // loop is unrolled once to ensure that `value' is positive

      output.write((int) (value & 0x7f));
      bytesWritten += 1;
      value >>>= 7;

      // while there are more than 6 bits to write, write 7 bit chunks:
      while (value > 0x3f) {
        output.write((int) (value & 0x7f));
        bytesWritten += 1;
        value >>>= 7;
      }

      // write the final 6 bits.
      output.write((int) (value | finalByteFlag));
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
