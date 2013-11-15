// BSD License (http://lemurproject.org/galago-license)
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
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringIteratorMaxDiffComparator;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import static org.lemurproject.galago.core.retrieval.processing.ProcessingModel.toReversedArray;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.FixedSizeMinHeap;
import org.lemurproject.galago.utility.Parameters;

/**
 * Assumes the use of delta functions for scoring, then prunes using Maxscore.
 * Generally this causes a substantial speedup in processing time.
 *
 * @author irmarc, sjh
 */
public class MaxScoreAggressiveModel2 extends ProcessingModel {

  LocalRetrieval retrieval;

  public MaxScoreAggressiveModel2(LocalRetrieval lr) {
    this.retrieval = lr;
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    ScoringContext context = new ScoringContext();
    int requested = (int) queryParams.get("requested", 1000);

    // step one: find the set of deltaScoringNodes in the tree
    List<Node> scoringNodes = new ArrayList();
    boolean canScore = findDeltaNodes(queryTree, scoringNodes, retrieval);
    if (!canScore) {
      throw new IllegalArgumentException("Query tree does not support delta scoring interface.\n" + queryTree.toPrettyString());
    }

    // step two: create an iterator for each node
    boolean shareNodes = false;
    List<DeltaScoringIterator> scoringIterators = createScoringIterators(scoringNodes, retrieval, shareNodes);

    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

    double maximumPossibleScore = 0.0;
    for (DeltaScoringIterator scorer : scoringIterators) {
      maximumPossibleScore += scorer.maximumWeightedScore();
    }

    // sentinel scores are set to collectionFrequency (sort function ensures decreasing order)
    Collections.sort(scoringIterators, new DeltaScoringIteratorMaxDiffComparator());

    // precompute statistics that allow us to update the quorum index
    double runningMaxScore = maximumPossibleScore;
    double[] maxScoreOfRemainingIterators = new double[scoringIterators.size()];
    for (int i = 0; i < scoringIterators.size(); i++) {
      maxScoreOfRemainingIterators[i] = runningMaxScore;
      runningMaxScore -= scoringIterators.get(i).maximumDifference();
    }

    int quorumIndex = scoringIterators.size();
    double minHeapThresholdScore = Double.NEGATIVE_INFINITY;

    // Main loop : 
    context.document = -1;

    while (true) {
      long candidate = Long.MAX_VALUE;
      for (int i = 0; i < scoringIterators.size(); i++) {
        // find a real candidate //
        // TODO: add a function that does this more efficiently //
        DeltaScoringIterator dsi = scoringIterators.get(i);
        scoringIterators.get(i).movePast(context.document);
        long c = scoringIterators.get(i).currentCandidate();
        while (!dsi.isDone() && !dsi.hasMatch(c)) {
          scoringIterators.get(i).movePast(c);
          c = scoringIterators.get(i).currentCandidate();
        }
        if (!dsi.isDone()) {
          candidate = (candidate < c) ? candidate : c;
        }
      }

      // Means sentinels are done, we can quit
      if (candidate == Long.MAX_VALUE) {
        break;
      }

      context.document = candidate;
      // Setup to score
      double runningScore = maximumPossibleScore;

      // score all iterators
      int i = 0;
      while (runningScore > minHeapThresholdScore && i < scoringIterators.size()) {
        DeltaScoringIterator dsi = scoringIterators.get(i);
        dsi.syncTo(candidate);
        runningScore -= dsi.deltaScore(context);
        ++i;
      }

      // Fully scored it
      if (i == scoringIterators.size()) {
        if (queue.size() < requested) {
          ScoredDocument scoredDocument = new ScoredDocument(candidate, runningScore);
          queue.offer(scoredDocument);

        } else if (runningScore > queue.peek().score) {
          ScoredDocument scoredDocument = new ScoredDocument(candidate, runningScore);
          queue.offer(scoredDocument);
          minHeapThresholdScore = queue.peek().score;

          // check if this update will allow us to discard an iterator from consideration : 
          while (quorumIndex > 0 && maxScoreOfRemainingIterators[(quorumIndex - 1)] < minHeapThresholdScore) {
            quorumIndex--;
          }
        }
      }
    }

    return toReversedArray(queue);
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
