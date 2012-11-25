/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class MinCountIterator extends ValueIterator implements MovableCountIterator {

  private final NodeParameters nodeParams;
  private final MovableCountIterator[] iterators;
  protected MovableIterator[] drivingIterators;
  protected boolean hasAllCandidates;
  protected ScoringContext context;

  public MinCountIterator(NodeParameters np, MovableCountIterator[] countIterators) {
    this.nodeParams = np;
    this.iterators = countIterators;
    // count the number of iterators that dont have
    // a non-default data for all candidates
    int drivingIteratorCount = 0;
    for (MovableIterator iterator : this.iterators) {
      if (!iterator.hasAllCandidates()) {
        drivingIteratorCount++;
      }
    }

    if (drivingIteratorCount <= 0) {
      // if all iterators will report matches for all documents
      // make sure this information is communicated up.
      hasAllCandidates = true;
      drivingIterators = iterators;

    } else {
      // otherwise this disjunction is discriminative
      // and will not report matches for all documents
      //
      // the driving iterators will ensure this iterator
      //   does not stop at ALL documents
      hasAllCandidates = false;
      drivingIterators = new MovableIterator[drivingIteratorCount];
      int i = 0;
      for (MovableIterator iterator : this.iterators) {
        if (!iterator.hasAllCandidates()) {
          drivingIterators[i] = iterator;
          i++;
        }
      }
    }
  }

  @Override
  public void syncTo(int candidate) throws IOException {
    for (MovableIterator iterator : iterators) {
      int prev = iterator.currentCandidate();
      iterator.syncTo(candidate);
    }
  }

  @Override
  public void movePast(int candidate) throws IOException {
    for (MovableIterator iterator : this.drivingIterators) {
      iterator.movePast(candidate);
    }
  }

  @Override
  public int currentCandidate() {
    int candidateMax = Integer.MIN_VALUE;
    int candidateMin = Integer.MAX_VALUE;
    for (MovableIterator iterator : drivingIterators) {
      if (iterator.isDone()) {
        return Integer.MAX_VALUE;
      }
      candidateMax = Math.max(candidateMax, iterator.currentCandidate());
      candidateMin = Math.min(candidateMin, iterator.currentCandidate());
    }
    if (candidateMax == candidateMin) {
      return candidateMax;
    } else {
      return candidateMax - 1;
    }
  }

  @Override
  public boolean hasMatch(int candidate) {
    for (MovableIterator iterator : drivingIterators) {
      if (iterator.isDone() || !iterator.hasMatch(candidate)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDone() {
    for (MovableIterator iterator : drivingIterators) {
      if (iterator.isDone()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void reset() throws IOException {
    for (MovableIterator iterator : iterators) {
      iterator.reset();
    }
  }

  @Override
  public boolean hasAllCandidates() {
    return hasAllCandidates;
  }

  @Override
  public long totalEntries() {
    long min = Integer.MAX_VALUE;
    for (MovableIterator iterator : iterators) {
      min = Math.min(min, iterator.totalEntries());
    }
    return min;
  }

  @Override
  public int compareTo(MovableIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return this.currentCandidate() - other.currentCandidate();
  }

  @Override
  public String getKeyString() throws IOException {
    return nodeParams.get("default", "missing-key");
  }

  @Override
  public byte[] getKeyBytes() throws IOException {
    return Utility.fromString(nodeParams.get("default", "missing-key"));
  }

  @Override
  public String getEntry() throws IOException {
    return getKeyString() + "," + count();
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public byte[] key() {
    return Utility.fromString(nodeParams.get("default", "missing-key"));
  }

  @Override
  public int count() {
    int count = Integer.MAX_VALUE;
    for (MovableCountIterator countItr : iterators) {
      count = Math.min(count, countItr.count());
    }
    count = (count == Integer.MAX_VALUE)? 0 : count;
    return count;
  }

  @Override
  public int maximumCount() {
    int maxCount = Integer.MAX_VALUE;
    for (MovableCountIterator countItr : iterators) {
      maxCount = Math.min(maxCount, countItr.maximumCount());
    }
    maxCount = (maxCount == Integer.MAX_VALUE)? 0 : maxCount;
    return maxCount;
  }

  @Override
  public void setContext(ScoringContext sc){
    this.context = sc;

    for(MovableIterator itr : this.iterators){
      itr.setContext(context);
    }
  }
}
