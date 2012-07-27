// BSD License (http://lemurproject.org/galago-license)
// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FileInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.lemurproject.galago.tupleflow.StreamCreator;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * A version of the UniversalParser which merely counts how many parseable
 * docs are in a particular split. This matters if you want to generate a
 * subcollection.
 *
 * @author irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class UniversalCounter extends StandardStep<DocumentSplit, KeyValuePair> {

  private Counter documentCounter;
  private Parameters parameters;
  private long count;
  private Logger LOG = Logger.getLogger(getClass().toString());
  private Closeable source;

  public UniversalCounter(TupleFlowParameters parameters) {
    documentCounter = parameters.getCounter("Documents Parsed");
    this.parameters = parameters.getJSON();
  }

  public void process(DocumentSplit split) throws IOException {
    DocumentStreamParser parser = null;
    source = null;

    // Determine the file type either from the parameters
    // or from the guess in the splits
    String fileType;
    if (parameters.containsKey("filetype")) {
      fileType = parameters.getString("filetype");
    } else {
      fileType = split.fileType;
    }

    try {
      if (fileType.equals("html")
              || fileType.equals("xml")
              || fileType.equals("txt")) {
        parser = new FileParser(parameters, split.fileName, getLocalBufferedReader(split));
      } else if (fileType.equals("arc")) {
        parser = new ArcParser(getLocalBufferedInputStream(split));
      } else if (fileType.equals("warc")) {
        parser = new WARCParser(getLocalBufferedInputStream(split));
      } else if (fileType.equals("trectext")) {
        parser = new TrecTextParser(getLocalBufferedReader(split));
      } else if (fileType.equals("trecweb")) {
        parser = new TrecWebParser(getLocalBufferedReader(split));
      } else if (fileType.equals("twitter")) {
        parser = new TwitterParser(getLocalBufferedReader(split));
      } else if (fileType.equals("corpus")) {
        parser = new CorpusSplitParser(split);
      } else if (fileType.equals("wiki")) {
        parser = new WikiParser(getLocalBufferedReader(split));
      } else if (fileType.equals("mbtei.page") || fileType.equals("mbtei")) {
        parser = new MBTEIPageParser(split, getLocalBufferedInputStream(split));
      } else if (fileType.equals("mbtei.book")) {
	parser = new MBTEIBookParser(split, getLocalBufferedInputStream(split));
      } else if (fileType.equals("mbtei.enitity")) {
	parser = new MBTEIEntityParser(split, getLocalBufferedInputStream(split));
      } else if (fileType.equals("mbtei.person")) {
	parser = new MBTEIPersonParser(split, getLocalBufferedInputStream(split));
      } else if (fileType.equals("mbtei.location")) {
	parser = new MBTEILocationParser(split, getLocalBufferedInputStream(split));
      } else {
        throw new IOException("Unknown fileType: " + fileType
                + " for fileName: " + split.fileName);
      }
    } catch (EOFException ee) {
      System.err.printf("Found empty split %s. Skipping due to no content.", split.toString());
      return;
    }
    Document document;
    count = 0;

    while ((document = parser.nextDocument()) != null) {
      count++;
      if (documentCounter != null) {
        documentCounter.increment();
      }
    }
    if (source != null) {
      source.close();
    }

    KeyValuePair kvp = new KeyValuePair();
    kvp.key = split.fileName.getBytes();
    kvp.value = Utility.compressLong(count);
    processor.process(kvp);
  }

  public static boolean isParsable(String extension) {
    return extension.equals("html")
            || extension.equals("xml")
            || extension.equals("txt")
            || extension.equals("arc")
            || extension.equals("warc")
            || extension.equals("trectext")
            || extension.equals("trecweb")
            || extension.equals("twitter")
            || extension.equals("corpus")
            || extension.equals("wiki")
            || extension.equals("mbtei");
  }

  public BufferedReader getLocalBufferedReader(DocumentSplit split) throws IOException {
    BufferedReader br = getBufferedReader(split);
    source = br;
    return br;
  }

  public static BufferedReader getBufferedReader(DocumentSplit split) throws IOException {
    FileInputStream stream = StreamCreator.realInputStream(split.fileName);
    BufferedReader reader;

    if (split.isCompressed) {
      // Determine compression type
      if (split.fileName.endsWith("gz")) { // Gzip
        reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(stream)));
      } else { // BZip2
        BufferedInputStream bis = new BufferedInputStream(stream);
        //bzipHeaderCheck(bis);
        reader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(bis)));
      }
    } else {
      reader = new BufferedReader(new InputStreamReader(stream));
    }
    return reader;
  }

  public BufferedInputStream getLocalBufferedInputStream(DocumentSplit split) throws IOException {
    BufferedInputStream bis = getBufferedInputStream(split);
    source = bis;
    return bis;
  }

  public static BufferedInputStream getBufferedInputStream(DocumentSplit split) throws IOException {
    FileInputStream fileStream = StreamCreator.realInputStream(split.fileName);
    BufferedInputStream stream;

    if (split.isCompressed) {
      // Determine compression algorithm
      if (split.fileName.endsWith("gz")) { // Gzip
        stream = new BufferedInputStream(new GZIPInputStream(fileStream));
      } else { // bzip2
        BufferedInputStream bis = new BufferedInputStream(fileStream);
        stream = new BufferedInputStream(new BZip2CompressorInputStream(bis));
      }
    } else {
      stream = new BufferedInputStream(fileStream);
    }
    return stream;
  }
}
