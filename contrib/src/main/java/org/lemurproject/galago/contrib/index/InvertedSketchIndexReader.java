/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.index;

import org.lemurproject.galago.contrib.hash.UniversalStringHashFunction;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.core.index.BTreeValueIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MinCountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.buffer.VByteInput;
import org.lemurproject.galago.utility.buffer.VByteOutput;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author sjh
 */
public class InvertedSketchIndexReader extends KeyListReader implements AggregateIndexPart {

  private int depth;
  private UniversalStringHashFunction[] hashFns;
  private Stemmer stemmer;

  public InvertedSketchIndexReader(BTreeReader reader) throws Exception {
    super(reader);
    init();
  }

  public InvertedSketchIndexReader(String pathname) throws Exception {
    super(pathname);
    init();
  }

  private void init() throws Exception {
    Parameters manifest = this.getManifest();
    stemmer = Stemmer.create(reader.getManifest());

    Parameters hfnParams = manifest.getMap("hashFns");
    depth = (int) manifest.getLong("depth");
    hashFns = new UniversalStringHashFunction[depth];
    for (int row = 0; row < depth; row++) {
      hashFns[row] = new UniversalStringHashFunction(hfnParams.getMap(Integer.toString(row)));
    }
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  /**
   * Returns an iterator pointing at the specified term, or null if the term
   * doesn't exist in the inverted file.
   */
  public MinCountIterator getTermCounts(byte[] key) throws IOException {
    ByteArrayOutputStream array;
    VByteOutput stream;
    TermCountIterator[] rowIterators = new TermCountIterator[depth];
    for (int row = 0; row < depth; row++) {
      array = new ByteArrayOutputStream();
      stream = new VByteOutput(new DataOutputStream(array));
      long hashValue = hashFns[row].hash(key);
      stream.writeLong(hashValue);
      stream.writeInt(row);

      BTreeIterator iterator = reader.getIterator(array.toByteArray());
      if (iterator == null) {
        return null;
      }

      rowIterators[row] = new TermCountIterator(iterator);
    }

    NodeParameters np = new NodeParameters();
    np.set("default", ByteUtil.toString(key));
    return new MinCountIterator(np, rowIterators);
  }

  public MinCountIterator getTermCounts(String term) throws IOException {
    return getTermCounts(ByteUtil.fromString(stemmer.stemAsRequired(term)));
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(MinCountIterator.class));
    return types;
  }

  @Override
  public BaseIterator getIterator(Node node) throws IOException {
    return null;
  }

  @Override
  public IndexPartStatistics getStatistics() {
    Parameters manifest = this.getManifest();
    IndexPartStatistics is = new IndexPartStatistics();
    is.collectionLength = manifest.get("statistics/collectionLength", 0);
    is.vocabCount = manifest.get("statistics/vocabCount", 0);
    is.highestDocumentCount = manifest.get("statistics/highestDocumentCount", 0);
    is.highestFrequency = manifest.get("statistics/highestFrequency", 0);
    is.partName = manifest.get("filename", "CountIndexPart");
    return is;
  }

  public static class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() throws IOException {
      TermCountIterator it;
      StringBuilder sb = new StringBuilder();
      sb.append(getKeyString()).append(",");
      sb.append("list of size: ");
      try {
        it = new TermCountIterator(iterator);
        long count = it.totalEntries();
        sb.append(count);
      } catch (IOException ioe) {
        sb.append("Unknown-count");
      }
      return sb.toString();
    }

    @Override
    public CountIterator getValueIterator() throws IOException {
      return new TermCountIterator(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      byte[] k = iterator.getKey();
      ByteArrayInputStream array = new ByteArrayInputStream(k);
      VByteInput stream = new VByteInput(new DataInputStream(array));
      long hashValue = stream.readLong();
      int row = stream.readInt();
      return row + "~" + hashValue;
    }
  }

  public static class TermCountIterator extends BTreeValueIterator
          implements NodeAggregateIterator, CountIterator {

    BTreeIterator iterator;
    int documentCount;
    int collectionCount;
    int maximumPositionCount;
    VByteInput documents;
    VByteInput counts;
    int documentIndex;
    int currentDocument;
    int currentCount;
    // to support skipping
    VByteInput skips;
    VByteInput skipPositions;
    DataStream skipPositionsStream;
    DataStream documentsStream;
    DataStream countsStream;
    int skipDistance;
    int skipResetDistance;
    long numSkips;
    long skipsRead;
    long nextSkipDocument;
    long lastSkipPosition;
    long documentsByteFloor;
    long countsByteFloor;

    public TermCountIterator(BTreeIterator iterator) throws IOException {
      super(iterator.getKey());
      reset(iterator);
    }

    // Initialization method.
    //
    // Even though we check for skips multiple times, in terms of how the data is loaded
    // its easier to do the parts when appropriate
    protected void initialize() throws IOException {
      DataStream valueStream = iterator.getSubValueStream(0, iterator.getValueLength());
      DataInput stream = new VByteInput(valueStream);

      // metadata
      int options = stream.readInt();
      documentCount = stream.readInt();
      collectionCount = stream.readInt();

      if ((options & HAS_MAXTF) == HAS_MAXTF) {
        maximumPositionCount = stream.readInt();
      } else {
        maximumPositionCount = Integer.MAX_VALUE;
      }

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipDistance = stream.readInt();
        skipResetDistance = stream.readInt();
        numSkips = stream.readLong();
      }

      // segment lengths
      long documentByteLength = stream.readLong();
      long countsByteLength = stream.readLong();
      long skipsByteLength = 0;
      long skipPositionsByteLength = 0;

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipsByteLength = stream.readLong();
        skipPositionsByteLength = stream.readLong();
      }

      long documentStart = valueStream.getPosition();
      long countsStart = documentStart + documentByteLength;
      long countsEnd = countsStart + countsByteLength;

      documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
      countsStream = iterator.getSubValueStream(countsStart, countsByteLength);

      documents = new VByteInput(documentsStream);
      counts = new VByteInput(countsStream);

      if ((options & HAS_SKIPS) == HAS_SKIPS) {

        long skipsStart = countsEnd;
        long skipPositionsStart = skipsStart + skipsByteLength;
        long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;

        assert skipPositionsEnd == iterator.getValueLength();

        skips = new VByteInput(iterator.getSubValueStream(skipsStart, skipsByteLength));
        skipPositionsStream = iterator.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
        skipPositions = new VByteInput(skipPositionsStream);

        // load up
        nextSkipDocument = skips.readInt();
        documentsByteFloor = 0;
        countsByteFloor = 0;
      } else {
        assert countsEnd == iterator.getValueLength();
        skips = null;
        skipPositions = null;
      }

      documentIndex = 0;
      load();
    }

    // Only loading the docid and the count
    private void load() throws IOException {
      currentDocument += documents.readInt();
      currentCount = counts.readInt();
    }

    @Override
    public String getValueString(ScoringContext sc) throws IOException {
      return getKeyString() + "," + currentDocument + "," + currentCount;
    }

    @Override
    public void reset(BTreeIterator i) throws IOException {
      iterator = i;
      key = iterator.getKey();
      initialize();
    }

    @Override
    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      initialize();
    }

    // If we have skips - it's go time
    @Override
    public void syncTo(long document) throws IOException {
      if (skips != null) {
        synchronizeSkipPositions();
        if (document > nextSkipDocument) {
          // if we're here, we're skipping
          while (skipsRead < numSkips
                  && document > nextSkipDocument) {
            skipOnce();
          }
          repositionMainStreams();
        }
      }


      // linear from here
      while (!isDone() && document > currentDocument) {
        documentIndex = Math.min(documentIndex + 1, documentCount);
        if (!isDone()) {
          load();
        }
      }
    }

    @Override
    public boolean hasMatch(ScoringContext context) {
      return !isDone() && currentCandidate() == context.document;
    }

    @Override
    public void movePast(long document) throws IOException {
      syncTo(document + 1);
    }

    // This only moves forward in tier 1, reads from tier 2 only when
    // needed to update floors
    //
    private void skipOnce() throws IOException {
      assert skipsRead < numSkips;
      long currentSkipPosition = lastSkipPosition + skips.readInt();

      if (skipsRead % skipResetDistance == 0) {
        // Position the skip positions stream
        skipPositionsStream.seek(currentSkipPosition);

        // now set the floor values
        documentsByteFloor = skipPositions.readInt();
        countsByteFloor = skipPositions.readInt();
      }
      currentDocument = (int) nextSkipDocument;

      // May be at the end of the buffer
      if (skipsRead + 1 == numSkips) {
        nextSkipDocument = Integer.MAX_VALUE;
      } else {
        nextSkipDocument += skips.readInt();
      }
      skipsRead++;
      lastSkipPosition = currentSkipPosition;
    }

    // This makes sure the skip list pointers are still ahead of the current document.
    // If we called "next" a lot, these may be out of sync.
    //
    private void synchronizeSkipPositions() throws IOException {
      while (nextSkipDocument <= currentDocument) {
        int cd = currentDocument;
        skipOnce();
        currentDocument = cd;
      }
    }

    private void repositionMainStreams() throws IOException {
      // If we just reset the floors, don't read the 2nd tier again
      if ((skipsRead - 1) % skipResetDistance == 0) {
        documentsStream.seek(documentsByteFloor);
        countsStream.seek(countsByteFloor);
      } else {
        skipPositionsStream.seek(lastSkipPosition);
        documentsStream.seek(documentsByteFloor + skipPositions.readInt());
        countsStream.seek(countsByteFloor + skipPositions.readInt());
        // we seek here, so no reading needed
      }
      documentIndex = (int) (skipDistance * skipsRead) - 1;
    }

    @Override
    public boolean isDone() {
      return documentIndex >= documentCount;
    }

    @Override
    public long currentCandidate() {
      return currentDocument;
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    @Override
    public int count(ScoringContext c) {
      if (currentDocument == c.document) {
        return currentCount;
      }
      return 0;
    }

    @Override
    public long totalEntries() {
      return documentCount;
    }

    @Override
    public NodeStatistics getStatistics() {
      NodeStatistics stats = new NodeStatistics();
      stats.node = ByteUtil.toString(this.key);
      stats.nodeFrequency = this.collectionCount;
      stats.nodeDocumentCount = this.documentCount;
      stats.maximumCount = this.maximumPositionCount;
      return stats;
    }

    @Override
    public AnnotatedNode getAnnotatedNode(ScoringContext c) {
      String type = "count";
      String className = this.getClass().getSimpleName();
      String parameters = "";
      long document = currentCandidate();
      boolean atCandidate = hasMatch(c);
      String returnValue = Integer.toString(count(c));
      List<AnnotatedNode> children = Collections.emptyList();

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }

    @Override
    public boolean indicator(ScoringContext c) {
      return count(c) > 0;
    }
  }
}
