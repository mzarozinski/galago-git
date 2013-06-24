// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.EarlyTerminationScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.core.scoring.JelinekMercerScorer;

/**
 *
 * @author irmarc
 */
@RequiredStatistics(statistics = {"maximumCount", "collectionLength", "nodeFrequency"})
@RequiredParameters(parameters = {"lambda"})
public class JelinekMercerScoringIterator extends ScoringFunctionIterator
        implements DeltaScoringIterator {

  double weight;
  int parentIdx;
  double min;
  double max;

  public JelinekMercerScoringIterator(NodeParameters p, LengthsIterator ls, CountIterator it)
          throws IOException {
    super(p, ls, it);
    this.setScoringFunction(new JelinekMercerScorer(p, it));
    weight = p.get("w", 1.0);
    parentIdx = (int) p.get("pIdx", 0);
    max = p.getLong("maximumCount");
    min = function.score(0, (int) p.getLong("maximumCount"));
  }

  @Override
  public double minimumScore() {
    return min;
  }

  public double getWeight() {
    return weight;
  }

  @Override
  public void deltaScore() {
    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;

    int count = ((CountIterator) iterator).count(context);

    double diff = weight * (function.score(count, this.lengthsIterator.length()) - max);
    ctx.runningScore += diff;
  }

  @Override
  public void maximumDifference() {
    EarlyTerminationScoringContext ctx = (EarlyTerminationScoringContext) context;
    double diff = weight * (min - max);
    ctx.runningScore += diff;
  }

  @Override
  public void aggregatePotentials(EarlyTerminationScoringContext ctx) {
    // Nothing to do
  }

  @Override
  public void setContext(ScoringContext ctx) {
    super.setContext(ctx);
    if (EarlyTerminationScoringContext.class.isAssignableFrom(ctx.getClass())) {
      EarlyTerminationScoringContext dctx = (EarlyTerminationScoringContext) ctx;
      if (dctx.members.contains(this)) {
        return;
      }
      dctx.scorers.add(this);
      dctx.members.add(this);
      dctx.startingPotential += (max * weight);
    }
  }
}
