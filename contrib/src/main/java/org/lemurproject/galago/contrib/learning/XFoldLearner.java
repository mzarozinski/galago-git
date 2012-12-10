/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.eval.QuerySetResults;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class XFoldLearner extends Learner {

  private int xfoldCount;
  private Map<Integer, Parameters> foldParameters;
  private Map<Integer, Learner> foldLearners;
  private Map<Integer, List<String>> trainQueryFolds;
  private Map<Integer, List<String>> testQueryFolds;
  private ArrayList queryNumbers;
  private boolean execute;

  public XFoldLearner(Parameters p, Retrieval r) throws Exception {
    super(p, r);

    // required parameters:
    assert (p.isLong("xfolds")) : this.getClass().getName() + " requires `xfolds' parameter, of type long";
    assert (!p.containsKey("xfoldLearner")
            || p.isString("xfoldLearner")) : this.getClass().getName() + " requires `xfoldLeaner' parameter, of type String";

    execute = p.get("execute", true);
    
    // create one set of parameters (and learner) for each xfold.
    xfoldCount = (int) p.getLong("xfolds");
    trainQueryFolds = new HashMap(xfoldCount);
    testQueryFolds = new HashMap(xfoldCount);
    foldParameters = new HashMap(xfoldCount);
    foldLearners = new HashMap(xfoldCount);

    // randomize order of queries
    queryNumbers = new ArrayList(this.queries.queryIdentifiers);
    Collections.shuffle(queryNumbers, random);

    // split queries into folds
    int foldSize = (int) Math.ceil((double) queryNumbers.size() / (double) xfoldCount);
    for (int foldId = 0; foldId < xfoldCount; foldId++) {
      List<String> xfoldQueryNumbers = queryNumbers.subList(foldId * foldSize, (foldId + 1) * foldSize);
      List<String> xfoldQueryNumbersInverse = new ArrayList(queryNumbers);
      xfoldQueryNumbersInverse.removeAll(xfoldQueryNumbers);

      outputTraceStream.println(String.format("Fold: %d contains %d + %d = %d queries", foldId, xfoldQueryNumbers.size(), xfoldQueryNumbersInverse.size(), this.queries.queryIdentifiers.size()));

      testQueryFolds.put(foldId, xfoldQueryNumbers);
      trainQueryFolds.put(foldId, xfoldQueryNumbersInverse);

      // create new learner for each fold
      // use the train queries for the fold
      Parameters copy = p.clone();
      copy.set("name", name + "-foldId-" + foldId);
      copy.set("learner", p.get("xfoldLearner", "default")); // overwrite //
      copy.remove("query");
      copy.remove("queries");
      copy.set("queries", queries.getParametersSubset(xfoldQueryNumbersInverse)); // overwrite //
      foldParameters.put(foldId, copy);

      if (outputFolder != null) {
        Utility.copyStringToFile(copy.toPrettyString(), new File(outputFolder, name + "-fold-" + foldId + ".json"));
      }

      foldLearners.put(foldId, LearnerFactory.instance(copy, retrieval));
    }

    // copy each one of these to a file .fold1, .fold2, ...
  }

  public void close() {
    super.close();
    for (Learner l : this.foldLearners.values()) {
      l.close();
    }
  }

  /**
   * learning function - returns a list of learnt parameters
   */
  @Override
  public RetrievalModelInstance learn() throws Exception {
    if (execute) {
      final List<RetrievalModelInstance> learntParams = new ArrayList();

      // one set of results per fold.
      for (int foldId : foldLearners.keySet()) {
        RetrievalModelInstance result = foldLearners.get(foldId).learn();
        double testScore = evaluateSpecificQueries(result, testQueryFolds.get(foldId));
        result.setAnnotation("testScore", Double.toString(testScore));
        double allScore = evaluateSpecificQueries(result, queryNumbers);
        result.setAnnotation("allScore", Double.toString(allScore));

        this.outputPrintStream.println(result.toString());

        learntParams.add(result);
      }

      // take an average value across fold instances
      Parameters settings = new Parameters();
      for (String param : this.learnableParameters.getParams()) {
        double setting = 0.0;
        for (RetrievalModelInstance foldOpt : learntParams) {
          setting += foldOpt.get(param);
        }
        setting /= learntParams.size();
        settings.set(param, setting);
      }
      RetrievalModelInstance averageParams = new RetrievalModelInstance(this.learnableParameters, settings);
      double score = evaluateSpecificQueries(averageParams, queryNumbers);
      averageParams.setAnnotation("score", Double.toString(score));
      averageParams.setAnnotation("name", name + "-xfold-avg");

      outputPrintStream.println(averageParams.toString());

      return averageParams;
    } else {
      outputPrintStream.println("NOT OPTIMIZING, returning random parameters.");
      return this.generateRandomInitalValues();
    }
  }

  protected double evaluateSpecificQueries(RetrievalModelInstance instance, List<String> qids) throws Exception {
    long start = 0;
    long end = 0;

    HashMap<String, ScoredDocument[]> resMap = new HashMap();

    // ensure the global parameters contain the current settings.
    Parameters settings = instance.toParameters();
    this.retrieval.getGlobalParameters().copyFrom(settings);

    for (String number : qids) {

      Node root = this.queries.getNode(number).clone();
      root = this.ensureSettings(root, settings);
      root = this.retrieval.transformQuery(root, settings);

      //  need to add queryProcessing params some extra stuff to 'settings'
      start = System.currentTimeMillis();
      ScoredDocument[] scoredDocs = this.retrieval.runQuery(root, settings);
      end = System.currentTimeMillis();

      if (scoredDocs != null) {
        resMap.put(number, scoredDocs);
      }
    }

    QuerySetResults results = new QuerySetResults(resMap);
    results.ensureQuerySet(queries.getParametersSubset(qids));
    double r = evalFunction.evaluate(results, qrels);

    outputTraceStream.println("Specific-query-set run time: " + (end - start) + ", settings : " + settings.toString() + ", score : " + r);

    return r;
  }
}
