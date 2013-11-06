// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk2;

import java.io.IOException;
import java.io.InputStream;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.CountSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.utility.compression.CompressedStreamFactory;
import org.lemurproject.galago.utility.compression.integer.CompressedLongReader;

/**
 *
 *
 * @author sjh
 * @see CountIndexReader
 */
public class CountIndexCountSource extends BTreeValueSource implements CountSource {

  // Statistics //
  long documentCount;
  long collectionCount;
  long maximumPositionCount;

  // Support for resets
  final long startPosition, endPosition;
  final String compression;

  // InputStreams
  InputStream documentsStream;
  InputStream countsStream;
  // CompressedStreams
  CompressedLongReader documents;
  CompressedLongReader counts;

  // variables for iteration
  long documentIndex;
  long currentDocument;
  int currentCount;
  boolean done;

  public CountIndexCountSource(BTreeReader.BTreeIterator iterator, String compression) throws IOException {
    super(iterator);
    this.compression = compression;
    this.startPosition = btreeIter.getValueStart();
    this.endPosition = btreeIter.getValueEnd();
    initialize();
  }

  @Override
  public void reset() throws IOException {
    initialize();
  }

  /**
   * Initialization method.
   *
   * Even though we check for skips multiple times, in terms of how the data is
   * loaded its easier to do the parts when appropriate. The magic number here
   * is a reasonable upper-bound on the header size (otherwise it reads 8k of
   * data redundantly)
   */
  protected void initialize() throws IOException {

    // header is no more than the first 256 bytes 
    InputStream header = (InputStream) btreeIter.getSubValueStream(0, 256);
    int headerLen = header.read();
    CompressedLongReader compressedHeader = CompressedStreamFactory.compressedLongStreamReaderInstance(compression, header);

    // sub-streams //
    long statsStart = headerLen + 1;
    long statsByteLength = compressedHeader.readLong();

    long documentByteStart = statsStart + statsByteLength;
    long documentByteLength = compressedHeader.readLong();

    long countsByteStart = documentByteStart + documentByteLength;
    long countsByteLength = compressedHeader.readLong();

    header.close();

    InputStream stats = (InputStream) btreeIter.getSubValueStream(statsStart, statsByteLength);
    CompressedLongReader compressedStats = CompressedStreamFactory.compressedLongStreamReaderInstance(compression, stats);

    documentCount = compressedStats.readLong(); // 9 bytes
    collectionCount = compressedStats.readLong(); // 9 bytes
    maximumPositionCount = compressedStats.readLong();  // 9 bytes

    documentsStream = (InputStream) btreeIter.getSubValueStream(documentByteStart, documentByteLength);
    documents = CompressedStreamFactory.compressedLongStreamReaderInstance(compression, documentsStream);

    countsStream = (InputStream) btreeIter.getSubValueStream(countsByteStart, countsByteLength);
    counts = CompressedStreamFactory.compressedLongStreamReaderInstance(compression, countsStream);

    assert (countsByteStart + countsByteLength == this.endPosition) : "count end should be the end of the value data";

    currentDocument = 0;
    currentCount = 0;
    done = false;

    documentIndex = 0;
    load();
  }

  private void load() throws IOException {
    if (documentIndex >= documentCount) {
      done = true;
      currentDocument = Long.MAX_VALUE;
      currentCount = 0;
      return;
    }
    currentDocument += documents.readLong();
    currentCount = counts.readInt();
  }

  @Override
  public boolean isDone() {
    return done;
  }

  @Override
  public boolean hasAllCandidates() {
    return false; //it's extremely unlikely
  }

  @Override
  public long totalEntries() {
    return documentCount;
  }

  @Override
  public long currentCandidate() {
    return currentDocument;
  }

  @Override
  public void movePast(long document) throws IOException {
    syncTo(document + 1);
  }

  @Override
  public void syncTo(long document) throws IOException {
    if (isDone()) {
      return;
    }

    // linear from here
    while (!isDone() && document > currentDocument) {
      documentIndex += 1;
      load();
    }
  }

  @Override
  public int count(long id) {
    if (!done && currentCandidate() == id) {
      return currentCount;
    }
    return 0;
  }

  @Override
  public NodeStatistics getStatistics() {
    NodeStatistics ns = new NodeStatistics();
    ns.node = this.key();
    ns.maximumCount = this.maximumPositionCount;
    ns.nodeFrequency = this.collectionCount;
    ns.nodeDocumentCount = this.documentCount;
    return ns;
  }
}
