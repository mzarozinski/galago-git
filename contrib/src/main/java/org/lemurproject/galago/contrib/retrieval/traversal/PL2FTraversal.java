/*
 * BSD License (http://www.galagosearch.org/license)

 */
package org.lemurproject.galago.contrib.retrieval.traversal;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.utility.Parameters;

import java.util.ArrayList;
import java.util.List;

/**
 * Transforms a #pl2f( text1 text2 ) node into the fully expanded PL2F model
 * described in "Combining Fields in Known-Item Email Search" by Macdonald and
 * Ounis.
 *
 * It's not the most elaborate description, but it's succinct and easy to
 * follow.
 *
 * Given f1 and f2, Expanded form should be something like:
 *
 * #combine:norm=false(
 *  #feature:dfr:qfmax=1:qf=1(
 *    # combine:norm=false(
 *      #feature:pl2f:lengths=f1( #counts:term1:part=field.f1() )
 *      #feature:pl2f:lengths=f2( #counts:term1:part=field.f2() )
 *    )
 *  )
 *  #feature:dfr:qfmax=1:qf=1(
 *    #combine:norm=false(
 *      #feature:pl2f:lengths=f1( #counts:term1:part=field.f1() )
 *      #feature:pl2f:lengths=f2( #counts:term1:part=field.f2() )
 *    )
 *  )
 * )
 * @author irmarc
 */
public class PL2FTraversal extends Traversal {

  int levels;
  List<String> fieldList;
  Parameters weights;
  Parameters smoothing;
  Parameters params;
  Parameters availableFields;
  TObjectIntHashMap<String> qTermCounts;
  int qfmax;
  Retrieval retrieval;

  public PL2FTraversal(Retrieval retrieval) {
    this.retrieval = retrieval;
    levels = 0;
    Parameters globals = retrieval.getGlobalParameters();
    params = globals.get("pl2f", Parameters.create());
    weights = params.get("weights", Parameters.create());
    smoothing = params.get("smoothing", Parameters.create());
    fieldList = globals.getAsList("fields", String.class);
    qTermCounts = new TObjectIntHashMap<String>();
    try {
      availableFields = retrieval.getAvailableParts();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beforeNode(Node object, Parameters qp) throws Exception {
    levels++;

    // If this is a text node, count it
    if (object.getOperator().equals("text") && object.getDefaultParameter() != null) {
      qTermCounts.adjustOrPutValue(object.getDefaultParameter(), 1, 1);
    }
  }

  @Override
  public Node afterNode(Node original, Parameters qp) throws Exception {
    levels--;
    if (levels == 0 && original.getOperator().equals("pl2f")) {
      qp.set("numberOfTerms", qTermCounts.keys().length);
      qp.set("numPotentials", qTermCounts.keys().length);
      qp.set("deltaWeightsSet", true);
      // Let's get qfmax
      int[] counts = qTermCounts.values();
      for (int count : counts) {
        qfmax = (count > qfmax) ? count : qfmax;
      }

      ArrayList<Node> termNodes = new ArrayList<Node>();

      int j = 0;
      for (Node child : original.getInternalNodes()) {
        termNodes.add(generatePL2FTermNode(child, j));
        j++;
      }

      // Top-level sums all term nodes
      Node termCombiner = new Node("combine", termNodes);
      termCombiner.getNodeParameters().set("norm", false);
      return termCombiner;
    } else {
      return original;
    }
  }

  private Node generatePL2FTermNode(Node n, int position) throws Exception {
    String term = n.getDefaultParameter();
    ArrayList<Node> fieldNodes = new ArrayList<Node>();
    NodeParameters fieldWeightParams = new NodeParameters();

    // For each term, generate F field nodes
    double normalizer = 0.0;
    for (int i = 0; i < fieldList.size(); i++) {
      String field = fieldList.get(i);

      // Make sure we have this field
      String partName = "field." + field;
      if (!availableFields.containsKey(partName)) {
        continue;
      }

      // Each field node is a count node wrapped in a feature:pl2f node.
      // Weights are added to a combine that sums the field values up.

      Node countNode = new Node("counts", term);
      countNode.getNodeParameters().set("part", partName);
      Node fieldNode = new Node("feature", "pl2f");
      NodeParameters np = fieldNode.getNodeParameters();
      np.set("lengths", field);
      np.set("pIdx", position);
      np.set("c", smoothing.get(field, params.get("smoothing_default", 0.5)));
      double w = weights.get(field, params.get("weight_default", 0.5));
      np.set("w", w);
      normalizer += w;
      fieldWeightParams.set(Integer.toString(i), w);
      fieldNode.addChild(countNode);
      fieldNodes.add(fieldNode);
    }
    //fieldWeightParams.set("norm", false);
    Node fieldCombiner = new Node("combine", fieldWeightParams, fieldNodes);
    // The above combine is a "tfn" value in the equation

    // the feature:dfr node applies the risk*gain function to the tfn.
    Node dfrNode = new Node("feature", "dfr");
    dfrNode.getNodeParameters().set("qf", qTermCounts.get(term));
    dfrNode.getNodeParameters().set("qfmax", qfmax);
    dfrNode.addChild(fieldCombiner);
    setTermStatistics(dfrNode, term, normalizer);
    return dfrNode;
  }

  private void setTermStatistics(Node dfr, String t, double normalizer) throws Exception {
    Node counter = new Node("counts", t);
    Node parted = TextPartAssigner.assignPart(counter,
            retrieval.getGlobalParameters(),
            retrieval.getAvailableParts());

    // get the frequency of the term in the collection:
    NodeStatistics ns = retrieval.getNodeStatistics(parted);
    dfr.getNodeParameters().set("nodeFrequency", ns.nodeFrequency);

    // get global document count:
    FieldStatistics cs = retrieval.getCollectionStatistics("#lengths:part=lengths()");
    dfr.getNodeParameters().set("documentCount", cs.documentCount);

    // Now echo these values down to the leaves
    List<Node> leaves = dfr.getInternalNodes().get(0).getInternalNodes();
    for (Node n : leaves) {
      n.getNodeParameters().set("nf", ns.nodeFrequency);
      n.getNodeParameters().set("dc", cs.documentCount);
      n.getNodeParameters().set("w", n.getNodeParameters().getDouble("w") / normalizer);
    }
  }
}
