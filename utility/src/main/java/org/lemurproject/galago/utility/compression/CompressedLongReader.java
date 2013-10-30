/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

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

  public void reset() throws IOException;
}
