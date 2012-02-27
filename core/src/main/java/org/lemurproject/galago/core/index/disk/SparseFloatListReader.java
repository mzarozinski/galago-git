// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.MovableValueIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Retrieves lists of floating point numbers which can be used as document features.
 * 
 * @author trevor
 */
public class SparseFloatListReader extends KeyListReader {

  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      ListIterator it;
      long count = -1;
      try {
        it = new ListIterator(iterator);
        count = it.totalEntries();
      } catch (IOException ioe) {
      }

      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(iterator.getKey())).append(", List Value: size=");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    @Override
    public ListIterator getValueIterator() throws IOException {
      return new ListIterator(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(iterator.getKey());
    }
  }

  public class ListIterator extends KeyListReader.ListIterator
          implements MovableScoreIterator {

    VByteInput stream;
    int documentCount;
    int index;
    int currentDocument;
    double currentScore;
    ScoringContext context;

    public ListIterator(BTreeReader.BTreeIterator iterator) throws IOException {
      super(iterator.getKey());
      reset(iterator);
    }

    void read() throws IOException {
      index += 1;

      if (index < documentCount) {
        currentDocument += stream.readInt();
        currentScore = stream.readFloat();
      }
    }

    @Override
    public String getEntry() {
      StringBuilder builder = new StringBuilder();

      builder.append(getKey());
      builder.append(",");
      builder.append(currentDocument);
      builder.append(",");
      builder.append(currentScore);

      return builder.toString();
    }

    @Override
    public boolean next() throws IOException {
      read();
      if (!isDone()) {
        return true;
      }
      return false;
    }

    @Override
    public void reset(BTreeReader.BTreeIterator iterator) throws IOException {
      DataStream buffered = iterator.getValueStream();
      stream = new VByteInput(buffered);
      documentCount = stream.readInt();
      index = -1;
      currentDocument = 0;
      if (documentCount > 0) {
        read();
      }
    }

    @Override
    public void reset() throws IOException {
      throw new UnsupportedOperationException("This iterator does not reset without the parent KeyIterator.");
    }

    @Override
    public void setContext(ScoringContext dc) {
      this.context = dc;
    }

    @Override
    public int currentCandidate() {
      return currentDocument;
    }
    
    @Override
    public boolean hasAllCandidates(){
      return false;
    }
    
    @Override
    public boolean moveTo(int document) throws IOException {
      while (!isDone() && document > currentDocument) {
        read();
      }
      return atCandidate(document);
    }

    @Override
    public double score(ScoringContext dc) {
      if (currentDocument == dc.document) {
        return currentScore;
      }
      return Double.NEGATIVE_INFINITY;
    }

    @Override
    public double score() {
      if (currentDocument == context.document) {
        return currentScore;
      }
      return Double.NEGATIVE_INFINITY;
    }

    @Override
    public boolean isDone() {
      return index >= documentCount;
    }

    @Override
    public long totalEntries() {
      return documentCount;
    }

    @Override
    public double maximumScore() {
      return Double.POSITIVE_INFINITY;
    }

    @Override
    public double minimumScore() {
      return Double.NEGATIVE_INFINITY;
    }
  }

  public SparseFloatListReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  public ListIterator getListIterator() throws IOException {
    return new ListIterator(reader.getIterator());
  }

  public ListIterator getScores(String term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(Utility.fromString(term));
    return new ListIterator(iterator);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> nodeTypes = new HashMap<String, NodeType>();
    nodeTypes.put("scores", new NodeType(ListIterator.class));
    return nodeTypes;
  }

  @Override
  public MovableValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("scores")) {
      return getScores(node.getDefaultParameter());
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }
}
