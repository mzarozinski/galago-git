// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.RequiredStatistics;
import org.lemurproject.galago.utility.Parameters;

/**
 * Class collects collections statistics: - collectionLength : number of terms
 * in index part / collection - documentCount : number of documents in index
 * part / collection - vocabCount : number of unique terms in index part -
 * nodeFrequency : number of matching instances of node in index part /
 * collection - nodeDocumentCount : number of matching documents for node in
 * index part / collection
 *
 * @author sjh
 */
public class BatchAnnotateCollectionStatistics extends Traversal {

  HashSet<String> availableColStatistics;
  HashSet<String> availableNodeStatistics;
  Parameters globalParameters;
  Retrieval retrieval;

  // featurefactory is necessary to get the correct class
  public BatchAnnotateCollectionStatistics(Retrieval retrieval) throws IOException {
    this.globalParameters = retrieval.getGlobalParameters();
    this.retrieval = retrieval;

    this.availableColStatistics = new HashSet<String>();
    // field or document region statistics
    this.availableColStatistics.add("collectionLength");
    this.availableColStatistics.add("documentCount");
    this.availableColStatistics.add("maxLength");
    this.availableColStatistics.add("minLength");
    this.availableColStatistics.add("avgLength");

    this.availableNodeStatistics = new HashSet<String>();
    // countable-node statistics
    this.availableNodeStatistics.add("nodeFrequency");
    this.availableNodeStatistics.add("nodeDocumentCount");
    this.availableNodeStatistics.add("maximumCount");
  }

  @Override
  public Node traverse(Node tree, Parameters queryParams) throws Exception {
    Map<Node, Set<String>> toAnnotate = new HashMap();
    Node replace = recurse(tree, toAnnotate);

    // annotate identified nodes
    annotate(toAnnotate, queryParams);

    return replace;
  }

  // Finds all nodes that require node or collection statistics //
  private Node recurse(Node node, Map<Node, Set<String>> toAnnotate) throws Exception {

    // need to get list of required statistics
    NodeType nt = retrieval.getNodeType(node);
    if (nt == null) {
      throw new IllegalArgumentException("NodeType of " + node.toString() + " is unknown.");
    }

    Class<? extends BaseIterator> c = nt.getIteratorClass();
    RequiredStatistics required = c.getAnnotation(RequiredStatistics.class);

    // then annotate the node with any of:
    // -- nodeFreq, nodeDocCount, collLen, docCount, collProb
    if (required != null) {
      HashSet<String> reqStats = new HashSet();
      for (String stat : required.statistics()) {
        NodeParameters np = new NodeParameters();
        if (!np.containsKey(stat) && (availableColStatistics.contains(stat) || availableNodeStatistics.contains(stat))) {
          reqStats.add(stat);
        }
      }
      if (!reqStats.isEmpty()) {
        toAnnotate.put(node, reqStats);
      }
    }

    // now recuse down //
    for (Node child : node.getInternalNodes()) {
      recurse(child, toAnnotate);
    }

    return node;
  }

  private void annotate(Map<Node, Set<String>> toAnnotate, Parameters qp) throws Exception {
    // divide the keyset into CollStats, and NodeStats
    Set<Node> colStatNodes = new HashSet();
    Set<Node> nodeStatNodes = new HashSet();

    for (Node node : toAnnotate.keySet()) {
      Set<String> reqStats = toAnnotate.get(node);

      for (String stat : reqStats) {
        if (availableColStatistics.contains(stat)) {
          colStatNodes.add(node);
          break;
        }
      }

      for (String stat : reqStats) {
        if (availableNodeStatistics.contains(stat)) {
          nodeStatNodes.add(node);
          break;
        }
      }
    }

    if (!colStatNodes.isEmpty()) {
      annotateColStats(toAnnotate, colStatNodes, qp);
    }

    if (!nodeStatNodes.isEmpty()) {
      annotateNodeStats(toAnnotate, nodeStatNodes, qp);
    }
  }

  public void annotateColStats(Map<Node, Set<String>> toAnnotate, Set<Node> colStatNodes, Parameters qp) throws Exception {
    Map<String, Node> lenNodes = new HashMap();

    // for now do something simple -- use caching //
    Map<String, Parameters> cache = new HashMap();
    for (Node n : colStatNodes) {

      // TODO: this needs to be smarter, to allow more interesting length nodes //
      String field = n.getNodeParameters().get("field", "document");
      Parameters s;
      if (!cache.containsKey(field)) {
        s = getCollectionStatistics(field, qp).toParameters();
        cache.put(field, s);
      } else {
        s = cache.get(field);
      }

      for (String reqStat : toAnnotate.get(n)) {
        if (s.isLong(reqStat)) {
          n.getNodeParameters().set(reqStat, s.getLong(reqStat));
        } else {
          n.getNodeParameters().set(reqStat, s.getDouble(reqStat));
        }
      }
    }
  }

  public void annotateNodeStats(Map<Node, Set<String>> toAnnotate, Set<Node> nodeStatNodes, Parameters qp) throws Exception {
    // Extract a minimal set of countable nodes (with mappings back to parent nodes) //
    Map<String, List<Node>> countableNodes = new HashMap();
    for (Node n : nodeStatNodes) {
      Node countableChild = locateCountNode(n);
      String countableChildString = countableChild.toString();
      if (!countableNodes.containsKey(countableChildString)) {
        countableNodes.put(countableChildString, new ArrayList());
      }
      countableNodes.get(countableChildString).add(n);
    }

    // collect ALL required NodeStatistic objects from index 
    Map<String, NodeStatistics> allNodeStats = collectStatistics(countableNodes.keySet(), qp);

    // Annotate nodes with stats
    for (String countableChildString : allNodeStats.keySet()) {
      Parameters stats = allNodeStats.get(countableChildString).toParameters();
      for (Node parent : countableNodes.get(countableChildString)) {
        for (String reqStat : toAnnotate.get(parent)) {
          if (stats.isLong(reqStat)) {
            parent.getNodeParameters().set(reqStat, stats.getLong(reqStat));
          } else {
            parent.getNodeParameters().set(reqStat, stats.getDouble(reqStat));
          }
        }
      }
    }
  }

  private FieldStatistics getCollectionStatistics(String field, Parameters qp) throws Exception {
    if (this.retrieval instanceof GroupRetrieval) {
      String group = qp.get("group", globalParameters.get("group", ""));
      // merge statistics "group" or read from "backgroundIndex"
      group = qp.get("backgroundIndex", globalParameters.get("backgroundIndex", group));

      if (!group.isEmpty()) {
        return ((GroupRetrieval) retrieval).getCollectionStatistics("#lengths:" + field + ":part=lengths()", group);
      }
    }
    return retrieval.getCollectionStatistics("#lengths:" + field + ":part=lengths()");
  }

  private Node locateCountNode(Node node) throws Exception {
    if (node.numChildren() == 2) {
      Node c = node.getChild(1);
      if (isCountNode(c)) {
        return c;
      }
    }
    throw new IllegalArgumentException("UNABLE TO FIND A COUNTABLE NODE FOR :\n" + node);
  }

  private Map<Node, NodeStatistics> collectStatistics(Collection<Node> countableNodes, Parameters qp) throws Exception {

    // we expect that each node has already been assigned a part //
    if (this.retrieval instanceof GroupRetrieval) {
      String group = qp.get("group", "");
      group = qp.get("backgroundIndex", group);
      if (!group.isEmpty()) {
        return ((GroupRetrieval) retrieval).getNodeStatistics(countableNodes, group);
      }
    }
    return retrieval.getNodeStatistics(countableNodes);
  }

  private boolean isCountNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return CountIterator.class.isAssignableFrom(outputClass);
  }

  @Override
  public void beforeNode(Node original, Parameters queryParameters) throws Exception {
    // unused
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    // unused
    return null;
  }
}
