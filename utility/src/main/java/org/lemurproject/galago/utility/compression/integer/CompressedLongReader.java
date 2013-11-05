/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression.integer;

import java.io.IOException;

/**
 * Interface for integer/long compression algorithms
 *
 *
 * @author sjh
 */
public interface CompressedLongReader {

  public int readInt() throws IOException;

  public long readLong() throws IOException;

  /**
   * Moves the underlying stream to the offset value, does nothing if the
   * absolute offset precedes current location
   *
   * @param absoluteOffset
   * @throws IOExcetion
   */
  public boolean seek(long absoluteOffset) throws IOException;
}
