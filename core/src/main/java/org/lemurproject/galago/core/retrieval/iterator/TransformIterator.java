// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.utility.Utility;

/**
 *
 * @author irmarc
 */
public abstract class TransformIterator implements BaseIterator {

  public BaseIterator iterator;

  public TransformIterator(BaseIterator iterator) {
    this.iterator = iterator;
  }

  @Override
  public void reset() throws IOException {
    iterator.reset();
  }

  @Override
  public boolean isDone() {
    return iterator.isDone();
  }

  @Override
  public boolean hasAllCandidates() {
    return iterator.hasAllCandidates();
  }

  @Override
  public long currentCandidate() {
    return iterator.currentCandidate();
  }

  @Override
  public boolean hasMatch(long identifier) {
    return iterator.hasMatch(identifier);
  }

  @Override
  public void syncTo(long identifier) throws IOException {
    iterator.syncTo(identifier);
  }

  @Override
  public void movePast(long identifier) throws IOException {
    iterator.movePast(identifier);
  }

  @Override
  public void findCandidatePast(long identifier) throws IOException {
    iterator.findCandidatePast(identifier);
  }

  @Override
  public String getValueString(ScoringContext c) throws IOException {
    return iterator.getValueString(c);
  }

  @Override
  public long totalEntries() {
    return iterator.totalEntries();
  }

  @Override
  public int compareTo(BaseIterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return Utility.compare(currentCandidate(), other.currentCandidate());
  }
}
