/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import org.lemurproject.galago.utility.compression.integer.CompressedLongReader;
import org.lemurproject.galago.utility.compression.integer.CompressedLongWriter;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.utility.Utility;

/**
 *
 * @author sjh
 */
public class CompressedWriterTest extends TestCase {

  public CompressedWriterTest(String testName) {
    super(testName);
  }

  public void testVByteWriter() throws Exception {
    compressor("vbyte");
  }
  public void testSignedVByteWriter() throws Exception {
    compressor("signed-vbyte");
  }
  public void testVByte2Writer() throws Exception {
    compressor("vbyte2");
  }
  public void testBPVByteWriter() throws Exception {
    compressor("bp-vbyte");
  }
  public void testNPFDVByteWriter() throws Exception {
    compressor("npfd-vbyte");
  }

  private void compressor(String compressor) throws IOException {
    File data = null;
    try {
      int streamSize = 16384;

      data = Utility.createTemporary();
      BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(data));

      CompressedLongWriter writer = CompressedStreamFactory.compressedLongStreamWriterInstance(compressor, out);

      int[] log = new int[streamSize];
      List<Long> skips = new ArrayList();
      List<Integer> skipLogLocations = new ArrayList();

      Random r = new Random(streamSize);

      for (int i = 0; i < log.length; i++) {
//        log[i] = Math.abs(r.nextInt(1024));
        log[i] = i;
        writer.writeLong(log[i]);

        if (i > 0 && i % 256 == 0) {
          skips.add(writer.getUnderlyingStreamPosition());
          skipLogLocations.add(i + 1);
        }
      }
      writer.close();

      // First verify that we can decompress data
      BufferedInputStream in = new BufferedInputStream(new FileInputStream(data));
      CompressedLongReader reader = CompressedStreamFactory.compressedLongStreamReaderInstance(compressor, in);

      for (int i = 0; i < log.length; i++) {
        long v = reader.readLong();
        assert (v == log[i]);
      }

      in = new BufferedInputStream(new FileInputStream(data));
      reader = CompressedStreamFactory.compressedLongStreamReaderInstance(compressor, in);
      // Next verify that we can skip data
      for (int i = 0; i < skips.size(); i++) {
        long offset = skips.get(i);
        int logOff = skipLogLocations.get(i);

        reader.seek(offset);
        long v = reader.readLong();
        assert (v == log[logOff]);
      }

    } finally {
      if (data != null) {
        data.delete();
      }
    }

  }
}
