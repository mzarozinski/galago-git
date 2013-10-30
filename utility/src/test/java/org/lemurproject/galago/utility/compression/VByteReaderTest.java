/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.utility.Utility;

/**
 *
 * @author sjh
 */
public class VByteReaderTest extends TestCase {

  public VByteReaderTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    File data = null;
    try {
      data = Utility.createTemporary();
      BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(data));

      CompressedLongWriter writer = new VByteWriter(out);
      long[] log = new long[1024];
      Random r = new Random(1024);
      for (int i = 0; i < log.length; i++) {
        log[i] = Math.abs(r.nextLong() / 2);
        writer.writeLong(log[i]);
      }
      writer.close();

      BufferedInputStream in = new BufferedInputStream(new FileInputStream(data));
      CompressedLongReader reader = new VByteReader(in);
      
      for (int i = 0; i < log.length; i++) {
        long v = reader.readLong();
        assert(v == log[i]);
      }
      
    } finally {
      if (data != null) {
        data.delete();
      }
    }
  }
}
