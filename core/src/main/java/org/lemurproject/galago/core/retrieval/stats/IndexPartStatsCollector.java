/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.stats;

import java.io.IOException;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.stats.AggregateIndexPart;
import org.lemurproject.galago.core.index.stats.AggregateStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class IndexPartStatsCollector extends StatisticsCollector {

  LocalRetrieval lr;

  public IndexPartStatsCollector(LocalRetrieval lr) {
    this.lr = lr;
  }

  @Override
  public AggregateStatistics collect(Node node, Parameters p) throws Exception {
    if (node.getNodeParameters().isString("part")) {
      return getPartStats(node.getNodeParameters().getString("part"));
    } else if (node.getOperator().equals("text")) {
      return getPartStats(node.getDefaultParameter());
    } else {
      throw new IllegalArgumentException("Could not identify an index part.\nExpected: #text:PART() or #count:key:part=PART()");
    }
  }

  private IndexPartStatistics getPartStats(String part) throws IOException {
    IndexPartReader p = lr.getIndexPart(part);
    if (AggregateIndexPart.class.isAssignableFrom(p.getClass())) {
      return ((AggregateIndexPart) p).getStatistics();
    }
    throw new IllegalArgumentException("Index part " + part + " does not extend AggregateIndexPart.");
  }
}
