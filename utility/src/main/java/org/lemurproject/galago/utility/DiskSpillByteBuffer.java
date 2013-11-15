// BSD License (http://lemurproject.org/galago-license)
/**
 * This is a file-backed byte buffer.
 *
 * We assume the buffer will have to work with variable sizes, so we start with
 * small allocation (2 bytes), and harmonically increase by a factor of 2 up to
 * sub-buffers of 32K. Starting w/ 2 provides the guarantee that if we write
 * anything the the buffer at all, our utilization will never be below 50%.
 *
 * Adding in copies of another buffer will not disrupt the harmonic progression,
 * because, there's no reason to believe we need to allocate larger sub-buffers.
 * It will, however, close out the current open sub-buffer if there is one and
 * open a new one of the same size.
 *
 * We also assume that we'll eventually get to a buffer size too big for memory.
 * Therefore, we spill to disk every 32MB. Once the overflow file is open, we
 * stream 32MB blocks to it. So, we never keep more than 32MB in memory. When
 * called to write, we close the file, let Utility.copyFileToStream do the work.
 * clear() will delete the temp file, otherwise it's cleared when galago cleans
 * up.
 *
 * @author sjh
 */
package org.lemurproject.galago.utility;

import gnu.trove.list.array.TIntArrayList;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DiskSpillByteBuffer extends OutputStream {

  List<byte[]> values = null;
  TIntArrayList sizes = null;
  FileOutputStream spillStream = null;
  File spillFile = null;
  long spillLength;
  byte[] currentBuffer;
  int currentBufferSize;
  int hi;
  int lo;
  int spillThreshold;
  long memoryLength;
  int position;
  private static final int BUFFER_LO = 2;
  private static final int BUFFER_HI = 32768;
  private static final int SPILL_SIZE = 33554432;

  public DiskSpillByteBuffer() {
    this(BUFFER_LO, BUFFER_HI, SPILL_SIZE);
  }

  public DiskSpillByteBuffer(int minimum, int maximum, int threshold) {
    assert (minimum <= maximum);
    lo = minimum;
    hi = maximum;
    spillThreshold = threshold;
    clear();
  }

  /**
   * Add a single byte to the buffer. This byte is written directly to the
   * buffer without compression.
   *
   * @param value The byte value to add.
   */
  @Override
  public void write(int value) {
    addByte(value);
  }

  @Override
  public void write(byte[] bytes) {
    write(bytes, 0, bytes.length);
  }

  @Override
  public void write(byte[] bytes, int offset, int length) {
    assert (offset + length <= bytes.length);
    for (int i = offset; i < (offset + length); i++) {
      addByte(bytes[i]);
    }
  }

  public void addByte(int value) {
    if (position == currentBufferSize) {
      values.add(currentBuffer);
      sizes.add(position);
      memoryLength += position;

      // check to see if we need to spill over
      if (memoryLength >= spillThreshold) {
        spill();
      }

      // Harmonic increase up to maximum
      if (currentBufferSize < hi) {
        currentBufferSize *= 2;
      }
      currentBuffer = new byte[currentBufferSize];
      position = 0;
    }

    if (currentBuffer == null) {
      currentBuffer = new byte[currentBufferSize];
      position = 0;
    }

    currentBuffer[position] = (byte) value;
    position += 1;
  }

  /**
   * Erases the contents of this buffer and sets its length to zero.
   */
  public void clear() {
    if (spillStream != null) {
      try {
        spillStream.close();
        spillFile.delete();
        spillFile = null;
        spillStream = null;
        spillLength = 0;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    if (values == null) {
      values = new ArrayList<byte[]>();
    } else {
      values.clear();
    }
    if (sizes == null) {
      sizes = new TIntArrayList();
    } else {
      sizes.clear();
    }
    currentBuffer = null;
    currentBufferSize = lo;
    position = 0;
    memoryLength = 0;
  }

  /**
   * Returns the length of the data stored in this buffer.
   */
  public long length() {
    return (spillLength + memoryLength + position);
  }

  /**
   * Writes the contents of this buffer to a stream.
   */
  public void write(OutputStream stream) throws IOException {
    // Copy spill file first.
    if (spillStream != null) {
      try {
        spillStream.close();
        Utility.copyFileToStream(spillFile, stream);
        spillStream = new FileOutputStream(spillFile); // maybe use RandomAccess here to do it?
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    // now what's backed in memory
    for (int i = 0; i < values.size(); i++) {
      stream.write(values.get(i), 0, sizes.get(i));
    }

    // And finally the latest buffer
    if (currentBuffer != null && position > 0) {
      stream.write(currentBuffer, 0, position);
    }
  }

  /**
   * Time to overflow into a temp file.
   */
  private void spill() {
    try {
      if (spillStream == null) {
        spillFile = Utility.createTemporary();
        spillStream = new FileOutputStream(spillFile);
      }
      // Spill everything in the array list, but not the current sub-buffer
      // We use sizes in case there was a buffer insertion (see add, above)
      for (int i = 0; i < values.size(); i++) {
        spillStream.write(values.get(i), 0, sizes.get(i));
      }
      spillLength += memoryLength;

      // Reset in-memory buffers
      values.clear();
      sizes.clear();
      memoryLength = 0;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
