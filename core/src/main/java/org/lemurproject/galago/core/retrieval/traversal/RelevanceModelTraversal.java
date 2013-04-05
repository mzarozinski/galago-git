// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.CallTable;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.core.scoring.RelevanceModel;
import org.lemurproject.galago.core.scoring.RelevanceModel.Gram;
import org.lemurproject.galago.core.scoring.WeightedTerm;
import org.lemurproject.galago.tupleflow.Utility;
import gnu.trove.list.array.TDoubleArrayList;
import java.util.Arrays;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.WordLists;
import org.lemurproject.galago.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

/**
 * The Relevance Model implemented as a traversal. Query should look like:
 *
 *  #rm:fbOrigWt=0.5:fbDocs=10:fbTerms=10( query )
 *
 * The outer node (the #rm operator) will be replaced with a #combine, and the 
 * query submitted to the retrieval supplied at construction. The parameters
 * will then be applied for constructing the expansion.
 *
 * @author irmarc
 */
public class RelevanceModelTraversal extends Traversal {

  Parameters globalParameters;
  Parameters queryParams;
  Parameters availableParts;
  Retrieval retrieval;

  public RelevanceModelTraversal(Retrieval retrieval, Parameters queryParams) throws IOException {
    this.queryParams = queryParams;
    this.retrieval = retrieval;

    this.globalParameters = retrieval.getGlobalParameters();
    this.availableParts = retrieval.getAvailableParts();
  }

  public Node afterNode(Node originalNode) throws Exception {
    if (originalNode.getOperator().equals("rm") == false) {
      return originalNode;
    }

    // Kick off the inner query
    NodeParameters parameters = originalNode.getNodeParameters();
    double fbOrigWt = parameters.get("fbOrigWt", 0.5);
    int fbDocs;
    // doubles allow learning module to operate over these parameters. -- default behaviour is to round to nearest integer.
    if(parameters.isDouble("fbDocs")){
      fbDocs = (int) Math.round(parameters.getDouble("fbDocs"));
    } else {
      fbDocs = (int) parameters.get("fbDocs", 10);
    }
    int fbTerms;
    if(parameters.isDouble("fbTerms")){
      fbTerms = (int) Math.round(parameters.getDouble("fbTerms"));
    } else {
      fbTerms = (int) parameters.get("fbTerms", 10);
    }
    
    // check parameters
    if(fbDocs <= 0){
      return originalNode;
    }
    
    if(fbTerms <= 0){
      return originalNode;
    }
    
    String operator = "combine";
    Node combineNode = new Node(operator, new NodeParameters(), Node.cloneNodeList(originalNode.getInternalNodes()), originalNode.getPosition());
    ArrayList<ScoredDocument> initialResults = new ArrayList<ScoredDocument>();

    // Only get as many as we need
    Parameters localParameters = globalParameters.clone();
    localParameters.set("requested", fbDocs);

    Node transformedCombineNode = retrieval.transformQuery(combineNode, localParameters);
    initialResults.addAll(Arrays.asList(retrieval.runQuery(transformedCombineNode, localParameters)));
    localParameters.set("parts", this.availableParts);

    RelevanceModel rModel = new RelevanceModel(localParameters, retrieval);
    rModel.initialize();
    
    Set<String> stopwords = WordLists.getWordList("rmstop");
    Set<String> queryTerms = StructuredQuery.findQueryTerms(combineNode);

    Node newRoot = null;
    Node expansionNode;

    expansionNode = rModel.generateExpansionQuery(initialResults, fbTerms, queryTerms, stopwords);
    NodeParameters expParams = new NodeParameters();
    expParams.set("0", fbOrigWt);
    expParams.set("1", 1 - fbOrigWt);
    ArrayList<Node> newChildren = new ArrayList<Node>();
    newChildren.add(combineNode);
    newChildren.add(expansionNode);
    newRoot = new Node("combine", expParams, newChildren, originalNode.getPosition());

    rModel.cleanup();
    return newRoot;
  }

  public void beforeNode(Node object) throws Exception {
  }
}
