/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.stats;

import org.lemurproject.galago.core.index.stats.AggregateStatistics;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class NodeStatsCollector extends StatisticsCollector {

  LocalRetrieval lr;

  public NodeStatsCollector(LocalRetrieval lr) {
    this.lr = lr;
  }

  @Override
  public AggregateStatistics collect(Node node, Parameters p) throws Exception {
    // node must correspond to a lengths iterator //
    BaseIterator i = lr.createIterator(p, node);

    if (NodeAggregateIterator.class.isAssignableFrom(i.getClass())) {
      // we have direct access to stats for this node
      return ((NodeAggregateIterator) i).getStatistics();

    } else if (CountIterator.class.isAssignableFrom(i.getClass())) {
      // we have to compute stats for this node
      NodeStatistics ns = new NodeStatistics();
      ns.node = node.toString();

      CountIterator ci = (CountIterator) i;
      ScoringContext sc = new ScoringContext();

      ns.maximumCount = 0;
      while (!ci.isDone()) {
        sc.document = ci.currentCandidate();
        if (ci.hasMatch(sc.document)) {
          int c = ci.count(sc);
          if (c > 0) {
            ns.nodeFrequency += c;
            ns.nodeDocumentCount += 1;
            ns.maximumCount = (ns.maximumCount >= c) ? ns.maximumCount : c;
          }
        }
        ci.movePast(sc.document);
      }

      return ns;
    } else {

      throw new IllegalArgumentException("Node must evaluate to a counts iterator.");
    }
  }
}
