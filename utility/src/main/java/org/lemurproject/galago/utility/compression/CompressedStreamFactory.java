/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 * @author sjh
 */
public class CompressedStreamFactory {

  public static CompressedLongWriter compressedLongStreamWriterInstance(String name, OutputStream stream) throws IOException {
    if (name.equals("vbyte")) {
      return new VByteWriter(stream);
    } else if (name.equals("bp-vbyte")) {
      return new GenericLemireCompressedWriter(stream);
    } else {
      System.err.println("Can not find compressor: " + name);
      return null;
    }
  }

  public static CompressedLongReader compressedLongStreamReaderInstance(String name, InputStream stream) throws IOException {
    if (name.equals("vbyte")) {
      return new VByteReader(stream);
    } else if (name.equals("bp-vbyte")) {
      return new GenericLemireCompressedReader(stream);
    } else {
      System.err.println("Can not find decompressor: " + name);
      return null;
    }
  }
}
