/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.utility.Utility;

/**
 *
 * @author sjh
 */
public class GenericLemireCompressedWriterTest extends TestCase {

  public GenericLemireCompressedWriterTest(String testName) {
    super(testName);
  }

  public void testSomeMethod() throws Exception {
    File data = null;
    try {
      data = Utility.createTemporary();
      BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(data));

      CompressedLongWriter writer = new GenericLemireCompressedWriter(out);
      int[] log = new int[2200];
      int[] dgaplog = new int[2200];
      Random r = new Random(2200);

      for (int i = 0; i < log.length; i++) {
        log[i] = Math.abs(r.nextInt(1024));
      }

      Arrays.sort(log, 0, log.length);

      int prev = 0;
      for (int i = 0; i < log.length; i++) {
        int val = log[i] - prev;        
        dgaplog[i] = val;
        writer.writeInt( val );
        prev = log[i];
      }
      writer.close();

      BufferedInputStream in = new BufferedInputStream(new FileInputStream(data));
      CompressedLongReader reader = new GenericLemireCompressedReader(in);

      prev = 0;
      for (int i = 0; i < log.length; i++) {
        int v = reader.readInt();
        prev = v;
//        System.err.println(v + " " + dgaplog[i]);
        assert (v == dgaplog[i]);
      }

    } finally {
      if (data != null) {
        data.delete();
      }
    }
  }
}
