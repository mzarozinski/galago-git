/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Very simple grid search learning algorithm - divides the n dimensional space
 * evenly using 'gridSize'. Searches from min to max for each dimension.
 *
 * Useful for very low values of n (e.g. 1,2,3) and low values of gridSize (
 * gridSize < 10 )
 *
 *
 *
 *
 *
 *
 *

 *
 * @author sjh
 */
public class GridSearchLearner extends Learner {

  double gridSize;

  public GridSearchLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // check required parameters
    assert (p.isLong("gridSize")) : this.getClass().getName() + " requires `gridSize' parameter, of type long";

    // collect local parameters
    gridSize = p.getLong("gridSize");
  }

  @Override
  public RetrievalModelInstance learn() throws Exception {
    List<String> params = new ArrayList(this.learnableParameters.getParams());
    RetrievalModelInstance settings;
    if (this.initialSettings.isEmpty()) {
      settings = super.generateRandomInitalValues();
    } else {
      settings = this.initialSettings.get(0);
    }
    settings.setAnnotation("name", name);

    if (params.size() > 0) {
      // depth first grid search
      RetrievalModelInstance best = gridLearn(0, params, settings);

      best.normalize();
      outputPrintStream.println(best.toString());
//      outputTraceStream.println("Best settings: " + best.toString());
      return best;
    }

    outputTraceStream.println("No learnable parameters found. Quitting.");
    return settings;
  }

  private RetrievalModelInstance gridLearn(int i, List<String> params, RetrievalModelInstance settings) throws Exception {

    String param = params.get(i);

    double paramValue = this.learnableParameters.getMin(param);
    double maxParamValue = this.learnableParameters.getMax(param);
    double step = this.learnableParameters.getRange(param) / (double) gridSize;

    double bestScore = 0.0;
    double bestParamValue = paramValue;

    while (paramValue <= maxParamValue) {
      settings.unsafeSet(param, paramValue);

      // recurse to find the best settings of the other parameters given this setting
      if (i + 1 < params.size()) {
        gridLearn(i + 1, params, settings);
      }

      double score = this.evaluate(settings);

      if (score > bestScore) {
        bestScore = score;
        bestParamValue = paramValue;
        outputTraceStream.println(String.format("Found better param value: %s = %f ; score = %f\nWhere settings: %s", param, paramValue, score, settings));
      }
      // move to the next grid line
      paramValue += step;
    }

    // ensure when we return the settings contain the best found parameter value
    settings.unsafeSet(param, bestParamValue);

    return settings;
  }
}
