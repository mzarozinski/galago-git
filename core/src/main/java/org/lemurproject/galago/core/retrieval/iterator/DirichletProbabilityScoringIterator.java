/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.FieldScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.DirichletProbabilityScorer;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * A ScoringIterator that makes use of the DirichletScorer function
 * for converting a count into a score.
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"collectionLength", "documentCount", "collectionProbability"})
public class DirichletProbabilityScoringIterator extends ScoringFunctionIterator {

  public double weight;
  public double max;
  public double min;
  public int parentIdx;
  String partName;

  public DirichletProbabilityScoringIterator(Parameters globalParams, NodeParameters p, MovableCountIterator it)
          throws IOException {
    super(it, new DirichletProbabilityScorer(globalParams, p, it));
    if (it instanceof PositionIndexReader.TermCountIterator) {
      PositionIndexReader.TermCountIterator maxIter = (PositionIndexReader.TermCountIterator) it;
      max = function.score(maxIter.maximumCount(), maxIter.maximumCount());
      //System.err.printf("%s max: %f\n", this.toString(), max);
    } else {
      max = 0;  // Means we have a null extent iterator
    }
    partName = p.getString("lengths");
    long collectionLength = p.getLong("collectionLength");
    long documentCount = p.getLong("documentCount");
    int avgDocLength = (int) Math.round(1.5 * (collectionLength + 0.0) / (documentCount + 0.0)); // Increase doc size by 50%
    min = function.score(0, avgDocLength); // Allows for a slightly more conservative "worst-case"
    parentIdx = -1;
  }

  public double maximumScore() {
    return max;
  }

  public double minimumScore() {
    return min;
  }

  @Override
  public boolean moveTo(int id) throws IOException {
    boolean result = super.moveTo(id);
    return result;
  }

  @Override
  public boolean next() throws IOException {
    boolean result = super.next();
    return result;
  }

  public void score(DeltaScoringContext ctx) {
    int count = 0;

    if (iterator.currentCandidate() == context.document) {
      count = ((CountIterator) iterator).count();
    }

    double diff = weight * (function.score(count, ((FieldScoringContext) context).lengths.get(partName)) - max);
    double newValue = ctx.potentials[parentIdx] + diff;
    
    ctx.runningScore += Math.log(newValue / ctx.potentials[parentIdx]);
    ctx.potentials[parentIdx] = newValue;

  }

  @Override
  public double score() {
    if (FieldScoringContext.class.isAssignableFrom(context.getClass())) {
      int count = 0;

      if (iterator.currentCandidate() == context.document) {
        count = ((CountIterator) iterator).count();
      }
      double score = function.score(count, ((FieldScoringContext) context).lengths.get(partName));

      return score;
    } else {
      return super.score();
    }
  }

  public void maximumAdjustment(DeltaScoringContext ctx) {
    double diff = weight * (min - max);
    double newValue = ctx.potentials[parentIdx] + diff;
    
    ctx.runningScore += Math.log(newValue / ctx.potentials[parentIdx]);
    ctx.potentials[parentIdx] = newValue;
  }
}
