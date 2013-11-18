/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.stats;

import org.lemurproject.galago.core.index.stats.AggregateStatistics;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class CollectionStatsCollector extends StatisticsCollector {

  LocalRetrieval lr;

  public CollectionStatsCollector(LocalRetrieval lr) {
    this.lr = lr;
  }

  @Override
  public AggregateStatistics collect(Node node, Parameters p) throws Exception {

    // node must correspond to a lengths iterator //
    NodeType nt = lr.getNodeType(node);

    if (nt != null && CollectionAggregateIterator.class.isAssignableFrom(nt.getIteratorClass())) {
      // we have direct access to stats for this node
      CollectionAggregateIterator i = (CollectionAggregateIterator) lr.createIterator(p, node);
      return i.getStatistics();

    } else if (nt != null && LengthsIterator.class.isAssignableFrom(nt.getIteratorClass())) {
      // we have to compute stats for this node
      FieldStatistics fs = new FieldStatistics();
      fs.fieldName = node.toString();
      LengthsIterator i = (LengthsIterator) lr.createIterator(p, node);
      ScoringContext sc = new ScoringContext();

      fs.firstDocId = i.currentCandidate();
      fs.maxLength = 0;
      fs.minLength = Integer.MAX_VALUE;
      while (!i.isDone()) {
        sc.document = i.currentCandidate();
        if (i.hasMatch(sc.document)) {
          int l = i.length(sc);
          fs.collectionLength += l;
          fs.documentCount += 1;
          fs.maxLength = (fs.maxLength >= l) ? fs.maxLength : l;
          fs.minLength = (fs.minLength <= l) ? fs.minLength : l;
          if (l > 0) {
            fs.nonZeroLenDocCount += 1;
          }
        }
        i.movePast(sc.document);
      }
      fs.lastDocId = sc.document;
      fs.avgLength = (double) fs.collectionLength / (double) fs.documentCount;

      return fs;

    } else {

      throw new IllegalArgumentException("Node must evaluate to a lengths iterator.");
    }
  }
}
