/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.compression;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.compression.CompressedLongReader;
import org.lemurproject.galago.utility.compression.CompressedLongWriter;
import org.lemurproject.galago.utility.compression.CompressedStreamFactory;

/**
 *
 * @author sjh
 */
public class CompressionTester {

  public static void main(String[] args) throws Exception {
    Parameters p = Parameters.parseArgs(args);
    File inputFile = new File(p.getString("input"));

    for (String compressor : (List<String>) p.getAsList("compressor")) {

      File outputFile = new File(inputFile.getAbsolutePath() + "." + compressor);

      Stats c1 = testCompression(inputFile, compressor, outputFile);
      Stats c2 = testCompression(inputFile, compressor, outputFile);
      Stats c3 = testCompression(inputFile, compressor, outputFile);
      Stats c = Stats.merge(new Stats[]{c1, c2, c3});

//      System.err.println(c.count);

      Stats d1 = testDecompression(outputFile, c1.count, compressor);
      Stats d2 = testDecompression(outputFile, c.count, compressor);
      Stats d3 = testDecompression(outputFile, c.count, compressor);
      Stats d = Stats.merge(new Stats[]{d1, d2, d3});

      // interesting stats :
      //   count //
      //   space //
      //   compression ratio  bits / integer //
      //   compression time //
      //   decompression time //
      System.out.format("%s : %d %d %.5f %.5f %.5f\n", compressor, c.count, c.space, ((double) (c.space * 8.0) / (double) c.count), c.compressTime, d.decompressTime);
      System.out.flush();
    }

  }

  private static Stats testCompression(File inputFile, String name, File outputFile) throws IOException {

    Stats s = new Stats();

    BufferedReader reader = new BufferedReader(new FileReader(inputFile));
    OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFile));

    long startTime = System.currentTimeMillis();

    String line;
    CompressedLongWriter compressor;
    compressor = CompressedStreamFactory.compressedLongStreamWriterInstance(name, out);
    while ((line = reader.readLine()) != null) {

      String[] parts = line.split("[,:]");
      for (String i : parts) {
        compressor.writeInt(Integer.parseInt(i));
        s.count++;
      }
    }

    long endTime = System.currentTimeMillis();
    compressor.close();

    s.compressTime = endTime - startTime;
    s.space = outputFile.length();

    return s;
  }

  private static Stats testDecompression(File compressedFile, long expectedCount, String name) throws IOException {
    Stats s = new Stats();

    InputStream out = new BufferedInputStream(new FileInputStream(compressedFile));
    DataInputStream dis = new DataInputStream(out);

    long startTime = System.currentTimeMillis();

    CompressedLongReader compressor;
    compressor = CompressedStreamFactory.compressedLongStreamReaderInstance(name, out);

    for (int i = 0; i < expectedCount; i++) {
      int value = compressor.readInt();
      s.count++;
    }

    long endTime = System.currentTimeMillis();
    out.close();

    s.decompressTime = endTime - startTime;

    return s;
  }

  private static class Stats {

    private static Stats merge(Stats[] stats) {
      Stats merged = new Stats();
      for (Stats s : stats) {
        merged.compressTime += s.compressTime;
        merged.decompressTime += s.decompressTime;

        if (merged.space == 0) {
          merged.space = s.space;
          merged.count = s.count;
        } else {
          assert (Math.abs((merged.space - s.space)) == 0);
          assert (Math.abs((merged.count - s.count)) == 0);
        }
      }

      merged.compressTime /= stats.length;
      merged.decompressTime /= stats.length;

      return merged;
    }

    public double compressTime = 0;
    public double decompressTime = 0;
    public long count = 0;
    public long space = 0;
  }
}
