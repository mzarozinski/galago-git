/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class TwoPassDocumentPassageModel extends ProcessingModel {

  private LocalRetrieval retrieval;
  private ProcessingModel firstPassDefault;
  private ProcessingModel firstPassMaxScore;
  private ProcessingModel secondPassDefault;
  private long topK = 10000;

  public TwoPassDocumentPassageModel(LocalRetrieval lr) {
    retrieval = lr;
    firstPassDefault = new RankedDocumentModel(retrieval);
    firstPassMaxScore = new MaxScoreDocumentModel(retrieval);
    secondPassDefault = new WorkingSetPassageModel(retrieval);
    topK = retrieval.getGlobalParameters().get("firstPassK", 2000);
  }

  @Override
  public List<ScoredPassage> executeQuery(Node queryTree, Parameters queryParams) throws Exception {
    Parameters firstPassParams = queryParams.clone();
    firstPassParams.set("requested", Math.max(topK, queryParams.get("requested", 1000)));
    // ensure the firstpass query is not mistaken for a delta ready query
    if (firstPassParams.containsKey("deltaReady")) {
      firstPassParams.remove("deltaReady");
    }

    // it would be nice to automatically generate these firstPassQueries,
    //   but there could be some difficultly in constructing a fast approximation
    //   of the query.
    Node firstPassQuery = StructuredQuery.parse(queryParams.getString("firstPassQuery"));
    firstPassQuery = retrieval.transformQuery(firstPassQuery, firstPassParams);

    List<ScoredDocument> results;
    if (firstPassParams.get("deltaReady", false)) {
      results = firstPassMaxScore.executeQuery(firstPassQuery, firstPassParams);
    } else {
      results = firstPassDefault.executeQuery(firstPassQuery, firstPassParams);
    }

    List<Long> workingSet = resultsToWorkingSet(results);
    queryParams.set("working", workingSet);

    List<ScoredPassage> passResults = secondPassDefault.executeQuery(queryTree, queryParams);

    return passResults;
  }

  private List<Long> resultsToWorkingSet(List<ScoredDocument> results) {
    List<Long> ws = new ArrayList();
    for (ScoredDocument d : results) {
      ws.add(d.document);
    }
    Collections.sort(ws);
    return ws;
  }
}
