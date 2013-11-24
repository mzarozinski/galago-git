/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.retrieval.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.DisjunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.FixedSizeMinHeap;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class HeuristicTwoPassMaxScore2PassiveModel extends ProcessingModel {

  private LocalRetrieval retrieval;
  private MaxScorePassiveModel2 maxScoreProcessor;

  public HeuristicTwoPassMaxScore2PassiveModel(LocalRetrieval lr) {
    this.retrieval = lr;
    this.maxScoreProcessor = new MaxScorePassiveModel2(lr);
  }

  @Override
  public List<ScoredDocument> executeQuery(Node queryTree, Parameters queryParams) throws Exception {

    ScoringContext context = new ScoringContext();
    int fpRequested = (int) queryParams.get("fpRequested", 1000);
    int requested = (int) queryParams.get("requested", 1000);

    // step one: find the set of deltaScoringNodes in the tree
    List<Node> scoringNodes = new ArrayList();
    boolean canScore = findDeltaNodes(queryTree, scoringNodes, retrieval);
    if (!canScore) {
      throw new IllegalArgumentException("Query tree does not support delta scoring interface.\n" + queryTree.toPrettyString());
    }

    List<Node> simpleScoringNodes = new ArrayList();
    for (Node scorer : scoringNodes) {
      if (scorer.getChild(1).numChildren() == 0) {
        simpleScoringNodes.add(scorer);
      }
    }

    if (simpleScoringNodes.isEmpty()) {
      simpleScoringNodes = scoringNodes;
    }

    if (simpleScoringNodes.size() == scoringNodes.size()) {
      fpRequested = requested;
    }

    FixedSizeMinHeap<ScoredDocument> queue = maxScoreProcessor.maxScore2Algorithm(simpleScoringNodes, fpRequested, queryParams);

    if (simpleScoringNodes.size() != scoringNodes.size()) {
      queue = secondPass(queue, scoringNodes, requested, queryParams);
    }

    return toReversedList(queue);
  }

  private FixedSizeMinHeap<ScoredDocument> secondPass(FixedSizeMinHeap<ScoredDocument> workingSetQueue,
          List<Node> scoringNodes,
          int requested,
          Parameters queryParams) throws Exception {

    ScoredDocument[] unsortedList = workingSetQueue.getUnsortedArray();
    List<Long> workingSet = new ArrayList(unsortedList.length);
    for (ScoredDocument sd : unsortedList) {
      workingSet.add(sd.document);
    }
    // ascending order
    Collections.sort(workingSet);

    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap(ScoredDocument.class, requested,
            new ScoredDocument.ScoredDocumentComparator());

    // passive
    boolean shareNodes = true;
    List<DeltaScoringIterator> scoringIterators = createScoringIterators(scoringNodes, retrieval, shareNodes);

    ScoringContext sc = new ScoringContext();

    for (Long doc : workingSet) {
      sc.document = doc;
      double score = 0;
      for (DeltaScoringIterator dsi : scoringIterators) {
        dsi.syncTo(doc);
        score += dsi.score(sc) * dsi.getWeight();
      }
      if (queue.size() < requested || score > queue.peek().score) {
        ScoredDocument scoredDocument = new ScoredDocument(doc, score);
        queue.offer(scoredDocument);
      }
    }

    return queue;
  }

  private boolean findDeltaNodes(Node n, List<Node> scorers, LocalRetrieval ret) throws Exception {
    // throw exception if we can't determine the class of each node.
    NodeType nt = ret.getNodeType(n);
    Class<? extends BaseIterator> iteratorClass = nt.getIteratorClass();

    if (DeltaScoringIterator.class.isAssignableFrom(iteratorClass)) {
      // we have a delta scoring class
      scorers.add(n);
      return true;

    } else if (DisjunctionIterator.class.isAssignableFrom(iteratorClass) && ScoreIterator.class.isAssignableFrom(iteratorClass)) {
      // we have a disjoint score combination node (e.g. #combine)
      boolean r = true;
      for (Node c : n.getInternalNodes()) {
        r &= findDeltaNodes(c, scorers, ret);
      }
      return r;

    } else {
      return false;
    }
  }

  private List<DeltaScoringIterator> createScoringIterators(List<Node> scoringNodes, LocalRetrieval ret, boolean shareNodes) throws Exception {
    List<DeltaScoringIterator> scoringIterators = new ArrayList();

    // the cache allows low level iterators to be shared
    Map<String, BaseIterator> queryIteratorCache;
    if (shareNodes) {
      queryIteratorCache = new HashMap();
    } else {
      queryIteratorCache = null;
    }
    for (int i = 0; i < scoringNodes.size(); i++) {
      DeltaScoringIterator scorer = (DeltaScoringIterator) ret.createNodeMergedIterator(scoringNodes.get(i), queryIteratorCache);
      scoringIterators.add(scorer);
    }
    return scoringIterators;
  }

}
