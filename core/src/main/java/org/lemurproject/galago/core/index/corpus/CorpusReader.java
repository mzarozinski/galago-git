// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * Reader for corpus folders
 *  - corpus folder is a parallel index structure:
 *  - one key.index file
 *  - several data files (0 -> n)
 *
 *
 * @author sjh
 */
public class CorpusReader extends KeyValueReader implements DocumentReader {

  boolean compressed;

  public CorpusReader(String fileName) throws FileNotFoundException, IOException {
    super(fileName);
    compressed = reader.getManifest().get("compressed", true);
  }

  public CorpusReader(BTreeReader r) {
    super(r);
    compressed = reader.getManifest().get("compressed", true);
  }
  
  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Document getDocument(int key) throws IOException {
    KeyIterator i = new KeyIterator(reader);
    byte[] k = Utility.fromInt(key);
    if (i.findKey(k)) {
      return i.getDocument();
    } else {
      return null;
    }
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("corpus", new NodeType(ValueIterator.class));
    return types;
  }

  @Override
  public ValueIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("corpus")) {
      return new CorpusIterator(new KeyIterator(reader));
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator implements DocumentIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    
    @Override
    public String getKeyString(){
      return Integer.toString(Utility.toInt(getKey()));
    }

    @Override
    public Document getDocument() throws IOException {
      return Document.deserialize(iterator.getValueBytes(), compressed);
    }
    
    @Override
    public String getValueString() throws IOException {
      try {
        return getDocument().toString();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      return new CorpusIterator(this);
    }

  }

  public class CorpusIterator extends KeyToListIterator implements DataIterator<Document> {

    public CorpusIterator(KeyIterator ki) {
      super(ki);
    }

    @Override
    public String getEntry() throws IOException {
      return ((KeyIterator) iterator).getDocument().toString();
    }

    @Override
    public long totalEntries() {
      return reader.getManifest().getLong("keyCount");
    }

    @Override
    public Document getData() {
      try {
        return ((KeyIterator) iterator).getDocument();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }
  }
}