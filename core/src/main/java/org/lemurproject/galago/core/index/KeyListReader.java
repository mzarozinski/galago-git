// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.iterator.ModifiableIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Base class for any data structures that map a key value
 * to a list of data, where one cannot assume the list can be
 * held in memory
 *
 *
 * @author irmarc
 */
public abstract class KeyListReader extends KeyValueReader {

  public KeyListReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
  }

  public KeyListReader(BTreeReader r) {
    super(r);
  }

  public abstract class ListIterator extends MovableValueIterator implements ModifiableIterator {

    // OPTIONS
    public static final int HAS_SKIPS = 0x01;
    public static final int HAS_MAXTF = 0x02;
    protected byte[] key;
    protected Map<String, Object> modifiers = null;

    public ListIterator(byte[] key) {
      this.key = key;
    }

    public abstract void reset(BTreeReader.BTreeIterator it) throws IOException;

    public String getKey() {
      return Utility.toString(key);
    }

    public byte[] getKeyBytes() {
      return key;
    }

    @Override
    public boolean atCandidate(int id) {
      return (!isDone() && currentCandidate() == id);
    }

    @Override
    public void movePast(int id) throws IOException {
      moveTo(id + 1);
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
  }
}
