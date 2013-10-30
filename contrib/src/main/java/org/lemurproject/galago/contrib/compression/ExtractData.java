/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.compression;

import java.io.BufferedWriter;
import java.io.FileWriter;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskExtentIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class ExtractData {

  public static void main(String[] args) throws Exception {
    Parameters p = Parameters.parseArgs(args);

    String posIndexPart = p.getString("input");
    PositionIndexReader posReader = (PositionIndexReader) DiskIndex.openIndexPart(posIndexPart);
    PositionIndexReader.KeyIterator ki = posReader.getIterator();

    String o = p.getString("output");
    String o1 = o + ".docs"; // 
    String o2 = o + ".counts";
    String o12 = o + ".doccounts";
    String o3 = o + ".positions";

    BufferedWriter w1 = new BufferedWriter(new FileWriter(o1));
    BufferedWriter w2 = new BufferedWriter(new FileWriter(o2));
    BufferedWriter w12 = new BufferedWriter(new FileWriter(o12));
    BufferedWriter w3 = new BufferedWriter(new FileWriter(o3));

    String sep1 = ",";
    String sep2 = ":";
    String sep3 = "\n";

    ScoringContext sc = new ScoringContext();

    while (!ki.isDone()) {
      DiskExtentIterator ei = ki.getValueIterator();

      boolean first = true;      
      while (!ei.isDone()) {
        sc.document = ei.currentCandidate();
        ExtentArray ea = ei.extents(sc);
        if (!first) {
          w1.write(sep1);
          w2.write(sep1);
          w12.write(sep1);
        }
        first = false;

        w1.write("" + sc.document);
        w2.write("" + ea.size());
        w12.write(sc.document + sep2 + ea.size());
        for (int j = 0; j < ea.size(); j++) {
          if(j > 0){
            w3.write(sep1);
          }
          w3.write("" + ea.begin(j));
        }

        w3.write("\n");
        
        ei.movePast(sc.document);
      }
      
      w1.write("\n");
      w2.write("\n");
      w12.write("\n");

      ki.nextKey();
    }

    w1.close();
    w2.close();
    w12.close();
    w3.close();

  }
}
