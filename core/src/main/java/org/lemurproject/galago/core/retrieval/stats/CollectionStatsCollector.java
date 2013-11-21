/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.stats;

import org.lemurproject.galago.core.index.stats.AggregateStatistics;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
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
    BaseIterator i = lr.createIterator(p, node);

    if (CollectionAggregateIterator.class.isAssignableFrom(i.getClass())) {
      // we have direct access to stats for this node
      return ((CollectionAggregateIterator) i).getStatistics();

    } else if (LengthsIterator.class.isAssignableFrom(i.getClass())) {

      LengthsIterator li = (LengthsIterator) i;
      // we have to compute stats for this node
      FieldStatistics fs = new FieldStatistics();
      fs.fieldName = node.toString();
      ScoringContext sc = new ScoringContext();

      fs.firstDocId = li.currentCandidate();
      fs.maxLength = 0;
      fs.minLength = Integer.MAX_VALUE;
      while (!li.isDone()) {
        sc.document = li.currentCandidate();
        if (li.hasMatch(sc.document)) {
          int l = li.length(sc);
          fs.collectionLength += l;
          fs.documentCount += 1;
          fs.maxLength = (fs.maxLength >= l) ? fs.maxLength : l;
          fs.minLength = (fs.minLength <= l) ? fs.minLength : l;
          if (l > 0) {
            fs.nonZeroLenDocCount += 1;
          }
        }
        li.movePast(sc.document);
      }
      fs.lastDocId = sc.document;
      fs.avgLength = (double) fs.collectionLength / (double) fs.documentCount;

      return fs;

    } else {

      throw new IllegalArgumentException("Node must evaluate to a lengths iterator.");
    }
  }
}
