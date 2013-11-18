/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.stats;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.core.index.stats.AggregateStatistics;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

/**
 * Interface similar to Query Processing Models.
 *
 * Each Statistical gatherer collects and returns statistics for index parts,
 * and length and count nodes
 *
 * @author sjh
 */
public abstract class StatisticsCollector {

  public abstract AggregateStatistics collect(Node node, Parameters p) throws Exception;

  /**
   * This function can be overridden to allow more efficient statistics
   * gathering.
   */
  public Map<Node, AggregateStatistics> collectAll(Collection<Node> node, Parameters p) throws Exception {
    Map<Node, AggregateStatistics> stats = new HashMap();
    for (Node n : node) {
      stats.put(n, collect(n, p));
    }
    return stats;
  }

  public static StatisticsCollector instance(LocalRetrieval r, Parameters p) throws Exception {
    if (p.isString("statCollector")) {
      String collector = p.getString("statCollector");
      // OPTION 1 : shorthand forms
      if (collector.equals("partStats")) {
        return new IndexPartStatsCollector(r);
      } else if (collector.equals("collStats")) {
        return new CollectionStatsCollector(r);
      } else if (collector.equals("nodeStats")) {
        return new NodeStatsCollector(r);

      } else {
        // OPTION 2 : explicit classpath
        Class sc = Class.forName(collector);
        assert (sc.isAssignableFrom(StatisticsCollector.class)) : "statCollector " + collector + " must be a classpath that extends StatisticsCollector, or a hardcoded shorthand name.";
        Constructor c = sc.getConstructor(LocalRetrieval.class);
        return (StatisticsCollector) c.newInstance(r);
      }
    }
    throw new IllegalArgumentException("A valid statCollector parameter must be provided in parameters.");
  }
}
