// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.lemurproject.galago.core.index.AggregateReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.AggregateReader.AggregateIterator;
import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.disk.PositionIndexWriter;
import org.lemurproject.galago.core.index.disk.TopDocsReader.TopDocument;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.ContextualIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ExtentValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.ModifiableIterator;
import org.lemurproject.galago.core.retrieval.processing.TopDocsContext;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Utility.ByteArrComparator;
import org.lemurproject.galago.tupleflow.VByteInput;


/*
 * author sjh
 *
 * In-memory posting index
 *
 */
public class MemoryPostings implements MemoryIndexPart, AggregateReader {

  // this could be a bit big -- but we need random access here
  // should use a trie (but java doesn't have one?)
  protected TreeMap<byte[], PostingList> postings = new TreeMap(new ByteArrComparator());
  protected Parameters parameters;
  protected long collectionDocumentCount = 0;
  protected long collectionPostingsCount = 0;
  protected Stemmer stemmer = null;

  public MemoryPostings(Parameters parameters) throws Exception {
    this.parameters = parameters;

    if (parameters.containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(parameters.getString("stemmer")).newInstance();
    }

    // if the parameters specify a collection length use them.
    collectionPostingsCount = parameters.get("statistics/collectionLength", 0);
    collectionDocumentCount = parameters.get("statistics/documentCount", 0);
  }

  // overridable function (for stemming etc) 
  public Document preProcessDocument(Document doc) throws IOException {
    return doc;
  }

  @Override
  public void addDocument(Document doc) throws IOException {
    collectionDocumentCount += 1;
    collectionPostingsCount += doc.terms.size();

    // stemming may shorten document
    doc = preProcessDocument(doc);

    int position = 0;
    for (String term : doc.terms) {
      String stem = stemAsRequired(term);
      addPosting(Utility.fromString(stem), doc.identifier, position);
      position += 1;
    }
  }

  @Override
  public void addIteratorData(ValueIterator iterator) throws IOException {
    // we expect that this iterator is a KeyListReader.ListIterator
    byte[] key = ((KeyListReader.ListIterator) iterator).getKeyBytes();

    if (postings.containsKey(key)) {
      // do nothing - we have already cached this data
      return;
    }

    do {
      int document = iterator.currentCandidate();
      ExtentArrayIterator extentsIterator = new ExtentArrayIterator(((ExtentValueIterator) iterator).extents());
      while (!extentsIterator.isDone()) {
        int begin = extentsIterator.currentBegin();
        addPosting(key, document, begin);
        extentsIterator.next();
      }
    } while (iterator.next());
  }

  protected void addPosting(byte[] byteWord, int document, int position) {
    if (!postings.containsKey(byteWord)) {
      PostingList postingList = new PostingList(byteWord);
      postings.put(byteWord, postingList);
    }

    PostingList postingList = postings.get(byteWord);
    postingList.add(document, position);
  }

  // Posting List Reader functions
  @Override
  public KeyIterator getIterator() throws IOException {
    return new KIterator();
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    String term = stemAsRequired(node.getDefaultParameter());
    byte[] byteWord = Utility.fromString(term);
    if (node.getOperator().equals("counts")) {
      return getTermCounts(byteWord);
    } else {
      return getTermExtents(byteWord);
    }
  }

  @Override
  public NodeStatistics getTermStatistics(String term) throws IOException {
    term = stemAsRequired(term);
    return getTermStatistics(Utility.fromString(term));
  }

  @Override
  public NodeStatistics getTermStatistics(byte[] term) throws IOException {
    PostingList postingList = postings.get(term);
    if (postingList != null) {
      CountsIterator counts = new CountsIterator(postingList);
      return counts.getStatistics();
    }
    NodeStatistics stats = new NodeStatistics();
    stats.node = Utility.toString(term);
    return stats;
  }

  private CountsIterator getTermCounts(byte[] term) throws IOException {
    PostingList postingList = postings.get(term);
    if (postingList != null) {
      return new CountsIterator(postingList);
    }
    return null;
  }

  private ExtentsIterator getTermExtents(byte[] term) throws IOException {
    PostingList postingList = postings.get(term);
    if (postingList != null) {
      return new ExtentsIterator(postingList);
    }
    return null;
  }

  // try to free up memory.
  @Override
  public void close() throws IOException {
    postings = null;
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("counts", new NodeType(CountsIterator.class));
    types.put("extents", new NodeType(ExtentsIterator.class));
    return types;
  }

  @Override
  public String getDefaultOperator() {
    return "extents";
  }

  @Override
  public Parameters getManifest() {
    return parameters;
  }

  @Override
  public long getDocumentCount() {
    return collectionDocumentCount;
  }

  @Override
  public long getCollectionLength() {
    return collectionPostingsCount;
  }

  @Override
  public long getVocabCount() {
    return postings.size();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest();
    p.set("filename", path);
    p.set("statistics/documentCount", this.getDocumentCount());
    p.set("statistics/collectionLength", this.getCollectionLength());
    p.set("statistics/vocabCount", this.getVocabCount());
    PositionIndexWriter writer = new PositionIndexWriter(new FakeParameters(p));

    KIterator kiterator = new KIterator();
    ExtentsIterator viterator;
    ExtentArray extents;
    while (!kiterator.isDone()) {
      viterator = (ExtentsIterator) kiterator.getValueIterator();
      writer.processWord(kiterator.getKey());

      while (!viterator.isDone()) {
        writer.processDocument(viterator.currentCandidate());
        extents = viterator.extents();
        for (int i = 0; i < extents.size(); i++) {
          writer.processPosition(extents.begin(i));
          writer.processTuple();
        }
        viterator.next();
      }
      kiterator.nextKey();
    }
    writer.close();
  }

  // private functions
  private String stemAsRequired(String term) {
    if (stemmer != null) {
      return stemmer.stem(term);
    }
    return term;
  }

  // sub classes:
  public class PostingList {

    byte[] key;
    CompressedByteBuffer documents_cbb = new CompressedByteBuffer();
    CompressedByteBuffer counts_cbb = new CompressedByteBuffer();
    CompressedByteBuffer positions_cbb = new CompressedByteBuffer();
    //IntArray documents = new IntArray();
    //IntArray termFreqCounts = new IntArray();
    //IntArray termPositions = new IntArray();
    int termDocumentCount = 0;
    int termPostingsCount = 0;
    int lastDocument = 0;
    int lastCount = 0;
    int lastPosition = 0;

    public PostingList(byte[] key) {
      this.key = key;
    }

    public void add(int document, int position) {
      if (termDocumentCount == 0) {
        // first instance of term
        lastDocument = document;
        lastCount = 1;
        termDocumentCount += 1;
        documents_cbb.add(document);
      } else if (lastDocument == document) {
        // additional instance of term in document
        lastCount += 1;
      } else {
        // new document
        assert lastDocument == 0 || document > lastDocument;
        documents_cbb.add(document - lastDocument);
        lastDocument = document;
        counts_cbb.add(lastCount);
        lastCount = 1;
        termDocumentCount += 1;
        lastPosition = 0;
      }
      assert lastPosition == 0 || position > lastPosition;
      positions_cbb.add(position - lastPosition);
      termPostingsCount += 1;
      lastPosition = position;
    }
  }
  // iterator allows for query processing and for streaming posting list data
  // public class Iterator extends ExtentIterator implements IndexIterator {

  public class KIterator implements KeyIterator {

    Iterator<byte[]> iterator;
    byte[] currKey;
    boolean done = false;

    public KIterator() throws IOException {
      iterator = postings.keySet().iterator();
      this.nextKey();
    }

    @Override
    public void reset() throws IOException {
      iterator = postings.keySet().iterator();
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(currKey);
    }

    @Override
    public byte[] getKey() {
      return currKey;
    }

    @Override
    public boolean nextKey() throws IOException {
      if (iterator.hasNext()) {
        currKey = iterator.next();
        return true;
      } else {
        currKey = null;
        done = true;
        return false;
      }
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      iterator = postings.tailMap(key).keySet().iterator();
      return nextKey();
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      iterator = postings.tailMap(key).keySet().iterator();
      return nextKey();
    }

    @Override
    public String getValueString() throws IOException {
      long count = -1;
      ExtentsIterator it = new ExtentsIterator(postings.get(currKey));
      count = it.count();
      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(getKey())).append(",");
      sb.append("list of size: ");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public int compareTo(KeyIterator t) {
      try {
        return Utility.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      if (currKey != null) {
        return new ExtentsIterator(postings.get(currKey));
      } else {
        return null;
      }
    }
  }

  public class ExtentsIterator implements ValueIterator, ModifiableIterator,
          AggregateIterator, CountValueIterator, ExtentValueIterator, ContextualIterator {

    PostingList postings;
    VByteInput documents_reader;
    VByteInput counts_reader;
    VByteInput positions_reader;
    int iteratedDocs;
    int currDocument;
    int currCount;
    ExtentArray extents;
    boolean done;
    ScoringContext context;
    Map<String, Object> modifiers;

    private ExtentsIterator(PostingList postings) throws IOException {
      this.postings = postings;
      reset();
    }

    @Override
    public void reset() throws IOException {
      documents_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.documents_cbb.getBytes())));
      counts_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.counts_cbb.getBytes())));
      positions_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.positions_cbb.getBytes())));

      iteratedDocs = 0;
      currDocument = 0;
      currCount = 0;
      extents = new ExtentArray();

      next();
    }

    @Override
    public int count() {
      return currCount;
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public ExtentArray extents() {
      return extents;
    }

    @Override
    public ExtentArray getData() {
      return extents;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public int currentCandidate() {
      return currDocument;
    }

    @Override
    public boolean atCandidate(int identifier) {
      return (!isDone() && identifier == currDocument);
    }

    @Override
    public boolean next() throws IOException {
      if (iteratedDocs >= postings.termDocumentCount) {
        done = true;
        return false;
      } else if (iteratedDocs == postings.termDocumentCount - 1) {
        currDocument = postings.lastDocument;
        currCount = postings.lastCount;
      } else {
        currDocument += documents_reader.readInt();
        currCount = counts_reader.readInt();
      }
      loadExtents();

      iteratedDocs++;
      return true;
    }

    public void loadExtents() throws IOException {
      extents.reset();
      extents.setDocument(currDocument);
      int position = 0;
      for (int i = 0; i < currCount; i++) {
        position += positions_reader.readInt();
        extents.add(position);
      }
    }

    @Override
    public boolean moveTo(int identifier) throws IOException {
      while (!isDone() && (currDocument < identifier)) {
        next();
      }
      return atCandidate(identifier);
    }

    @Override
    public void movePast(int identifier) throws IOException {
      moveTo(identifier + 1);
    }

    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(Utility.toString(postings.key));
      builder.append(",");
      builder.append(currDocument);
      for (int i = 0; i < extents.size(); ++i) {
        builder.append(",");
        builder.append(extents.begin(i));
      }

      return builder.toString();
    }

    @Override
    public long totalEntries() {
      return postings.termDocumentCount;
    }

    @Override
    public NodeStatistics getStatistics() {
      if (modifiers != null && modifiers.containsKey("background")) {
        return (NodeStatistics) modifiers.get("background");
      }
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(postings.key);
      stats.nodeFrequency = postings.termPostingsCount;
      stats.nodeDocumentCount = postings.termDocumentCount;
      stats.collectionLength = collectionPostingsCount;
      stats.documentCount = collectionDocumentCount;
      return stats;
    }

    @Override
    public int compareTo(ValueIterator other) {
      if (isDone() && !other.isDone()) {
        return 1;
      }
      if (other.isDone() && !isDone()) {
        return -1;
      }
      if (isDone() && other.isDone()) {
        return 0;
      }
      return currentCandidate() - other.currentCandidate();
    }

    @Override
    public void addModifier(String k, Object m) {
      if (modifiers == null) {
        modifiers = new HashMap<String, Object>();
      }
      modifiers.put(k, m);
    }

    @Override
    public Set<String> getAvailableModifiers() {
      return modifiers.keySet();
    }

    @Override
    public boolean hasModifier(String key) {
      return ((modifiers != null) && modifiers.containsKey(key));
    }

    @Override
    public Object getModifier(String modKey) {
      if (modifiers == null) {
        return null;
      }
      return modifiers.get(modKey);
    }

    @Override
    public ScoringContext getContext() {
      return this.context;
    }

    // This will pass up topdocs information if it's available
    @Override
    public void setContext(ScoringContext context) {
      if ((context != null) && TopDocsContext.class.isAssignableFrom(context.getClass())
              && this.hasModifier("topdocs")) {
        ((TopDocsContext) context).hold = ((ArrayList<TopDocument>) getModifier("topdocs"));
        // remove the pointer to the mod (don't need it anymore)
        this.modifiers.remove("topdocs");
      }
      this.context = context;
    }
  }

  public class CountsIterator implements ValueIterator, ModifiableIterator,
          AggregateIterator, CountValueIterator, ContextualIterator {

    PostingList postings;
    VByteInput documents_reader;
    VByteInput counts_reader;
    int iteratedDocs;
    int currDocument;
    int currCount;
    boolean done;
    ScoringContext context;
    Map<String, Object> modifiers;

    private CountsIterator(PostingList postings) throws IOException {
      this.postings = postings;
      reset();
    }

    @Override
    public void reset() throws IOException {
      documents_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.documents_cbb.getBytes())));
      counts_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.counts_cbb.getBytes())));

      iteratedDocs = 0;
      currDocument = 0;
      currCount = 0;

      next();
    }

    @Override
    public int count() {
      return currCount;
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public int currentCandidate() {
      return currDocument;
    }

    @Override
    public boolean atCandidate(int identifier) {
      return (!isDone() && identifier == currDocument);
    }

    @Override
    public boolean next() throws IOException {
      if (iteratedDocs >= postings.termDocumentCount) {
        done = true;
        return false;
      } else if (iteratedDocs == postings.termDocumentCount - 1) {
        currDocument = postings.lastDocument;
        currCount = postings.lastCount;
      } else {
        currDocument += documents_reader.readInt();
        currCount = counts_reader.readInt();
      }

      iteratedDocs++;
      return true;
    }

    @Override
    public boolean moveTo(int identifier) throws IOException {
      while (!isDone() && (currDocument < identifier)) {
        next();
      }
      return atCandidate(identifier);
    }

    @Override
    public void movePast(int identifier) throws IOException {
      moveTo(identifier + 1);
    }

    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(Utility.toString(postings.key));
      builder.append(",");
      builder.append(currDocument);
      builder.append(",");
      builder.append(currCount);

      return builder.toString();
    }

    @Override
    public long totalEntries() {
      return postings.termDocumentCount;
    }

    @Override
    public NodeStatistics getStatistics() {
      if (modifiers != null && modifiers.containsKey("background")) {
        return (NodeStatistics) modifiers.get("background");
      }
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(postings.key);
      stats.nodeFrequency = postings.termPostingsCount;
      stats.nodeDocumentCount = postings.termDocumentCount;
      stats.collectionLength = collectionPostingsCount;
      stats.documentCount = collectionDocumentCount;
      return stats;
    }

    @Override
    public int compareTo(ValueIterator other) {
      if (isDone() && !other.isDone()) {
        return 1;
      }
      if (other.isDone() && !isDone()) {
        return -1;
      }
      if (isDone() && other.isDone()) {
        return 0;
      }
      return currentCandidate() - other.currentCandidate();
    }

    @Override
    public void addModifier(String k, Object m) {
      if (modifiers == null) {
        modifiers = new HashMap<String, Object>();
      }
      modifiers.put(k, m);
    }

    @Override
    public Set<String> getAvailableModifiers() {
      return modifiers.keySet();
    }

    @Override
    public boolean hasModifier(String key) {
      return ((modifiers != null) && modifiers.containsKey(key));
    }

    @Override
    public Object getModifier(String modKey) {
      if (modifiers == null) {
        return null;
      }
      return modifiers.get(modKey);
    }

    @Override
    public ScoringContext getContext() {
      return this.context;
    }

    // This will pass up topdocs information if it's available
    @Override
    public void setContext(ScoringContext context) {
      if ((context != null) && TopDocsContext.class.isAssignableFrom(context.getClass())
              && this.hasModifier("topdocs")) {
        ((TopDocsContext) context).hold = ((ArrayList<TopDocument>) getModifier("topdocs"));
        // remove the pointer to the mod (don't need it anymore)
        this.modifiers.remove("topdocs");
      }
      this.context = context;
    }
  }
}
