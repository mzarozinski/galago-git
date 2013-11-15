// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk2;

import org.lemurproject.galago.utility.DiskSpillByteBuffer;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.lemurproject.galago.core.index.BTreeWriter;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.index.disk.DiskBTreeWriter;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.utility.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.compression.CompressedStreamFactory;
import org.lemurproject.galago.utility.compression.integer.CompressedLongWriter;

/**
 * Count Indexes are similar to a Position or Window Index Writer, except that
 * extent positions are not written.
 *
 * Structure: mapping( term -> list(document-id), list(document-freq) )
 *
 * Skip lists are supported
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberWordCount", order = {"+word", "+document"})
public class CountIndexWriter implements
        NumberWordCount.WordDocumentOrder.ShreddedProcessor {

  // writer variables //
  Parameters actualParams;
  BTreeWriter writer;
  CountsList invertedList;
  String compression;
  // statistics //
  byte[] lastWord;
  long vocabCount = 0;
  long collectionLength = 0;
  long highestFrequency = 0;
  long highestDocumentCount = 0;
  // skipping parameters
//  int options = 0;
//  int skipDistance;
//  int skipResetDistance;

  /**
   * Creates a new instance of CountIndexWriter
   */
  public CountIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    this.actualParams = parameters.getJSON();
    this.actualParams.set("writerClass", CountIndexWriter.class.getName());
    this.actualParams.set("readerClass", CountIndexReader.class.getName());
    this.actualParams.set("defaultOperator", "counts");

    this.writer = new DiskBTreeWriter(parameters);

    this.compression = this.actualParams.get("compression", "vbyte");

    // look for skips
//    boolean skip = parameters.getJSON().get("skipping", true);
//    this.skipDistance = (int) parameters.getJSON().get("skipDistance", 500);
//    this.skipResetDistance = (int) parameters.getJSON().get("skipResetDistance", 20);
  }

  @Override
  public void processWord(byte[] wordBytes) throws IOException {
    if (invertedList != null) {
      highestDocumentCount = (highestDocumentCount > invertedList.totalDocuments) ? highestDocumentCount : invertedList.totalDocuments;
      highestFrequency = (highestFrequency > invertedList.totalCount) ? highestFrequency : invertedList.totalCount;

      collectionLength += invertedList.totalCount;
      invertedList.close();
      writer.add(invertedList);

      invertedList = null;
    }

    invertedList = new CountsList(compression, wordBytes);

    assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
    lastWord = wordBytes;

    vocabCount++;
  }

  @Override
  public void processDocument(long document) throws IOException {
    invertedList.addDocument(document);
  }

  @Override
  public void processTuple(int count) throws IOException {
    invertedList.addCount(count);
  }

  @Override
  public void close() throws IOException {
    if (invertedList != null) {
      highestDocumentCount = (highestDocumentCount > invertedList.totalDocuments) ? highestDocumentCount : invertedList.totalDocuments;
      highestFrequency = (highestFrequency > invertedList.totalCount) ? highestFrequency : invertedList.totalCount;

      collectionLength += invertedList.totalCount;
      invertedList.close();

      writer.add(invertedList);
    }

    // Add stats to the manifest if needed
    Parameters manifest = writer.getManifest();
    manifest.set("statistics/collectionLength", collectionLength);
    manifest.set("statistics/vocabCount", vocabCount);
    manifest.set("statistics/highestDocumentCount", highestDocumentCount);
    manifest.set("statistics/highestFrequency", highestFrequency);

    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("CountIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    writer.setProcessor(processor);
  }

  public class CountsList implements IndexElement {

    public byte[] key;
    private long prevDocument;
    private long currentDocumentFrequency;
    private long totalDocuments;
    private long totalCount;
    private long maximumDocumentFrequency;

    private byte[] header;
    private DiskSpillByteBuffer stats;
    private DiskSpillByteBuffer documents;
    private DiskSpillByteBuffer counts;
    private CompressedLongWriter compressedStats;
    private CompressedLongWriter compressedDocuments;
    private CompressedLongWriter compressedCounts;

    private final String compressionType;
    private boolean empty;
    private boolean closed;

    public CountsList(String compressionType, byte[] key) throws IOException {
      stats = new DiskSpillByteBuffer();
      documents = new DiskSpillByteBuffer();
      counts = new DiskSpillByteBuffer();

      compressedStats = CompressedStreamFactory.compressedLongStreamWriterInstance(compressionType, stats);
      compressedDocuments = CompressedStreamFactory.compressedLongStreamWriterInstance(compressionType, documents);
      compressedCounts = CompressedStreamFactory.compressedLongStreamWriterInstance(compressionType, counts);

      empty = true;
      closed = false;

      this.compressionType = compressionType;
      this.key = key;
      this.prevDocument = 0;
      this.totalCount = 0;
      this.currentDocumentFrequency = 0;
      this.maximumDocumentFrequency = 0;
    }

    /**
     * Prepares CountList for writing
     *
     * @throws IOException
     */
    public void close() throws IOException {

      // write the final count //
      if (!empty) {
        compressedCounts.writeLong(currentDocumentFrequency);
        maximumDocumentFrequency = (maximumDocumentFrequency > currentDocumentFrequency)
                ? maximumDocumentFrequency : currentDocumentFrequency;
      }

      compressedStats.writeLong(totalDocuments);
      compressedStats.writeLong(totalCount);
      compressedStats.writeLong(maximumDocumentFrequency);

      // ensure the compressed streams are flushed //
      compressedCounts.flush();
      compressedDocuments.flush();
      compressedStats.flush();

      
      // write the header
      ByteArrayOutputStream headerStream = new ByteArrayOutputStream();
      CompressedLongWriter compressedHeader = CompressedStreamFactory.compressedLongStreamWriterInstance(compressionType, headerStream);
      compressedHeader.writeLong(stats.length());
      compressedHeader.writeLong(documents.length());
      compressedHeader.writeLong(counts.length());
      compressedHeader.close();
      
      header = headerStream.toByteArray();

      closed = true;
    }

    @Override
    public long dataLength() {
      if (closed) {
        long listLength = 0;

        listLength += header.length + 1;
        listLength += stats.length();
        listLength += counts.length();
        listLength += documents.length();

        return listLength;

      } else {
        return 0;
      }
    }

    @Override
    public void write(final OutputStream output) throws IOException {
      // write a single
      assert (header.length < 256) : "Three longs should never require more than a byte.";

      output.write(header.length);
      for (byte b : header) {
        output.write(b);
      }

      stats.write(output);
      stats.clear();

      documents.write(output);
      documents.clear();

      counts.write(output);
      counts.clear();

    }

    @Override
    public byte[] key() {
      return key;
    }

    public void addDocument(long documentID) throws IOException {

      // add the last document's counts
      if (documents.length() > 0) {
        compressedCounts.writeLong(currentDocumentFrequency);
        maximumDocumentFrequency = (maximumDocumentFrequency > currentDocumentFrequency)
                ? maximumDocumentFrequency : currentDocumentFrequency;
      }
      compressedDocuments.writeLong(documentID - prevDocument);
      prevDocument = documentID;

      currentDocumentFrequency = 0;
      totalDocuments++;
      
      empty = false;
    }

    public void addCount(int count) throws IOException {
      currentDocumentFrequency += count;
      totalCount += count;
    }
  }
}
