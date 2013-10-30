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
public interface CompressedLongWriter {

  public void writeInt(int value) throws IOException;

  public void writeLong(long value) throws IOException;

  /**
   * Flush stream -- ensures all memory/state is flushed
   * 
   * @throws IOException 
   */
  public void flush() throws IOException;

  /**
   * Closes compressed stream.
   * ONLY USE THIS FUNCTION WHEN THE UNDERLYING STREAM SHOULD BE CLOSED.
   * 
   * @throws IOException 
   */
  public void close() throws IOException;
}
