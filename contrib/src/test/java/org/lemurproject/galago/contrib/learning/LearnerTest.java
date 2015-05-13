/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import org.junit.Test;
import org.lemurproject.galago.contrib.util.TestingUtils;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.disk.SourceIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class LearnerTest {
  @Test
  public void testLearnerCaching() throws Exception {
    File index = null;
    File qrels = null;

    try {
      File[] files = TestingUtils.make10DocIndex();
      assertTrue(files[0].delete());
      FSUtil.deleteDirectory(files[1]); // corpus not required
      index = files[2]; // index is required
      qrels = FileUtility.createTemporary();

      Retrieval ret = RetrievalFactory.instance(index.getAbsolutePath(), Parameters.parseArray(
          "cache", true,
          "flattenCombine", true,
          "cacheScores", true));

      String qrelData =
              "q1 x 2 1\n"
              + "q1 x 5 1\n"
              + "q1 x 8 1\n"
              + "q2 x 3 1\n"
              + "q2 x 7 1\n";
      StreamUtil.copyStringToFile(qrelData, qrels);

      // init learn params with queries
      Parameters learnParams = Parameters.parseString("{\"queries\": [{\"number\":\"q1\",\"text\":\"#sdm( jump moon )\"}, {\"number\":\"q2\",\"text\":\"#sdm( everything shoe )\"}]}");
      learnParams.set("learner", "grid");
      learnParams.set("qrels", qrels.getAbsolutePath());
      // add two parameters
      List<Parameters> learnableParams = new ArrayList<>();
      learnableParams.add(Parameters.parseArray("name", "uniw", "max", 1.0, "min", -1.0));
      learnableParams.add(Parameters.parseArray("name", "odw", "max", 1.0, "min", -1.0));
      learnableParams.add(Parameters.parseArray("name", "uww", "max", 1.0, "min", -1.0));
      learnParams.set("learnableParameters", learnableParams);
      // add sum rule to ensure sums to 1
      Parameters normalRule = Parameters.create();
      normalRule.set("mode", "sum");
      normalRule.set("params", Arrays.asList(new String[]{"0", "1"}));
      normalRule.set("value", 1D);
      learnParams.set("normalization", Collections.singletonList(normalRule));

      learnParams.set("gridSize", 3);
      learnParams.set("restarts", 1);

      Learner learner = LearnerFactory.instance(learnParams, ret);

      // List<Parameters> params = learner.learn();
      // for (Parameters p : params) {
      //  System.err.println(p);
      // }

      assertNotNull(learner);
      LocalRetrieval r = (LocalRetrieval) learner.retrieval;

      // generate some new random parameters
      RetrievalModelInstance rnd = learner.generateRandomInitalValues();
      Parameters settings = rnd.toParameters();

      for (String number : learner.queries.getQueryNumbers()) {
        Node root = learner.queries.getNode(number).clone();
        r.getGlobalParameters().copyFrom(settings);
        root = learner.ensureSettings(root, settings);
        root = r.transformQuery(root, settings);

        // check which nodes have been cached
        // System.out.println(root.toPrettyString());

        // node is an SDM - root, children, and sub-children are not cached, nodes below that level are cached
        BaseIterator i = r.createIterator(Parameters.create(), root);
        assertFalse(i instanceof SourceIterator); // not disk level
        for (Node child : root.getInternalNodes()) {
          i = r.createIterator(Parameters.create(), child);
          assertFalse(i instanceof SourceIterator); // not disk level
          for (Node subchild : child.getInternalNodes()) {
            i = r.createIterator(Parameters.create(), subchild);
            SourceIterator si = (SourceIterator) i;
            assertTrue(si.getSource().getClass().getName().contains(".mem.")); // uses a memory source in iterator.
          }
        }
      }


    } finally {
      if (index != null) {
        FSUtil.deleteDirectory(index);
      }
      if (qrels != null) {
        assertTrue(qrels.delete());
      }
    }
  }
}
