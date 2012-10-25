// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.BufferedFileDataStream;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * 
 * @author trevor
 */
public class VocabularyReader {

  public static class IndexBlockInfo {
    public int slotId;
    public byte[] firstKey;
    public byte[] nextSlotKey;
    public long begin;
    public long length;
    public int headerLength;
  }
  List<IndexBlockInfo> slots;

  public VocabularyReader(BufferedFileDataStream input, long valueDataEnd) throws IOException {
    slots = new ArrayList<IndexBlockInfo>();
    read(input, valueDataEnd);
  }

  public IndexBlockInfo getSlot(int id) {
    if(id == slots.size()){
      return null;
    }
    return slots.get(id);
  }

  // needed for DocumentSource - should be fixed //
  public List<IndexBlockInfo> getSlots() {
    return slots;
  }

  public void read(BufferedFileDataStream input, long valueDataEnd) throws IOException {
    long last = 0;

    int finalKeyLength = input.readInt();
    byte[] finalIndexKey = new byte[finalKeyLength];
    input.readFully(finalIndexKey);

    while (input.getPosition() < input.length()) {
      // read - length of block key
      int length = Utility.uncompressInt(input);

      // read - block key
      byte[] data = new byte[length];
      input.readFully(data);
      
      // read  - offset of block
      long offset = Utility.uncompressLong(input);
      
      // read - length of block header 
      int headerLength = Utility.uncompressInt(input);

      // save this info
      IndexBlockInfo slot = new IndexBlockInfo();

      // set the length of the previous block, and a forward reference to this block
      if (slots.size() > 0) {
        slots.get(slots.size() - 1).length = offset - last;
        slots.get(slots.size() - 1).nextSlotKey = data;
      }

      slot.headerLength = headerLength;
      slot.begin = offset;
      slot.firstKey = data;
      slot.slotId = slots.size();

      slots.add(slot);
      last = offset;
    }

    if (slots.size() > 0) {
      IndexBlockInfo finalSlot = slots.get(slots.size()-1);
      finalSlot.length = valueDataEnd - finalSlot.begin;
      finalSlot.nextSlotKey = finalIndexKey;
    }
    assert valueDataEnd >= last;
  }

  public IndexBlockInfo get(byte[] key) {
    return get(key, 0);
  }

  public IndexBlockInfo get(byte[] key, int minBlock) {
    if (slots.isEmpty()) {
      return null;
    }
    int big = slots.size() - 1;
    int small = minBlock;

    while (big - small > 1) {
      int middle = small + (big - small) / 2;
      byte[] middleKey = slots.get(middle).firstKey;

      if (Utility.compare(middleKey, key) <= 0) {
        small = middle;
      } else {
        big = middle;
      }
    }

    IndexBlockInfo one = slots.get(small);
    IndexBlockInfo two = slots.get(big);

    if (Utility.compare(two.firstKey, key) <= 0) {
      return two;
    } else {
      return one;
    }
  }
}
