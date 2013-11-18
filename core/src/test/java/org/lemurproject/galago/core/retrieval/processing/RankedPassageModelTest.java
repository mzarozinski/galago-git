/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.Utility;

/**
 *
 * @author sjh
 */
public class RankedPassageModelTest extends TestCase {

  File corpus = null;
  File index = null;

  public RankedPassageModelTest(String testName) {
    super(testName);
  }

  @Override
  public void setUp() {
    try {
      corpus = Utility.createTemporary();
      index = Utility.createTemporaryDirectory();
      makeIndex(corpus, index);
    } catch (Exception e) {
      tearDown();
    }
  }

  @Override
  public void tearDown() {
    try {
      if (corpus != null) {
        corpus.delete();
      }
      if (index != null) {
        Utility.deleteDirectory(index);
      }
    } catch (Exception e) {
    }
  }

  public void testEntireCollection() throws Exception {
    Parameters globals = new Parameters();
    globals.set("passageQuery", true);
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = new Parameters();
    int req = 10;
    queryParams.set("requested", req);
    queryParams.set("passageQuery", true);
    queryParams.set("passageSize", 10);
    queryParams.set("passageShift", 5);

    Node query = StructuredQuery.parse("#combine( test text 0 1 )");
    query = ret.transformQuery(query, queryParams);

    RankedPassageModel model = new RankedPassageModel(ret);

    List<ScoredPassage> results = model.executeQuery(query, queryParams);

    // --- all documents contain these terms in the first ten words --
    // -> this query should only ever return the first passage (0-10)
    // -> and the top 100 scores should be equal
    assertEquals(results.size(), req);
    for (int i = 0; i < req; i++) {
      assertEquals(results.get(i).document, i);
      assertEquals(results.get(i).begin, 0);
      assertEquals(results.get(i).end, 10);
      assertEquals(results.get(i).rank, i + 1);
      if (i > 0) {
        assert (Utility.compare(results.get(i).score, results.get(i - 1).score) == 0);
      }
    }

    query = StructuredQuery.parse("#combine( test text 99 )");
    query = ret.transformQuery(query, queryParams);

    results = model.executeQuery(query, queryParams);

    // note that dirichlet favours smaller documents over longer documents 
    assertEquals(results.size(), req);
    assertEquals(results.get(0).document, 94);
    assertEquals(results.get(0).begin, 100);
    assertEquals(results.get(0).end, 106);
    assertEquals(results.get(0).rank, 1);
    assertEquals(results.get(0).score, -4.776027, 0.000001);

    assertEquals(results.get(1).document, 90);
    assertEquals(results.get(1).begin, 95);
    assertEquals(results.get(1).end, 102);
    assertEquals(results.get(1).rank, 2);
    assertEquals(results.get(1).score, -4.776691, 0.000001);

    assertEquals(results.get(2).document, 95);
    assertEquals(results.get(2).begin, 100);
    assertEquals(results.get(2).end, 107);
    assertEquals(results.get(2).rank, 3);
    assertEquals(results.get(2).score, -4.776691, 0.000001);

  }

  public void testWhiteList() throws Exception {
    Parameters globals = new Parameters();
    globals.set("passageQuery", true);
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = new Parameters();
    queryParams.set("requested", 100);
    queryParams.set("passageQuery", true);
    queryParams.set("passageSize", 10);
    queryParams.set("passageShift", 5);

    Node query = StructuredQuery.parse("#combine( test text 0 1 )");
    query = ret.transformQuery(query, queryParams);

    WorkingSetPassageModel model = new WorkingSetPassageModel(ret);
    queryParams.set("working",
            Arrays.asList(new Long[]{2l, 3l, 4l, 5l, 6l, 7l, 8l, 9l, 10l, 11l}));

    List<ScoredPassage> results = model.executeQuery(query, queryParams);

    // --- all documents contain these terms in the first ten words --
    // -> this query should only ever return the first passage (0-10)
    // -> and all scores should be equal
    assertEquals(31, results.size());
    for (int i = 0; i < 10; i++) {
      assertEquals(results.get(i).document, i + 2);
      assertEquals(results.get(i).begin, 0);
      assertEquals(results.get(i).end, 10);
      assertEquals(results.get(i).rank, i + 1);
      assertEquals(results.get(i).score, -4.085500, 0.000001);
    }

    query = StructuredQuery.parse("#combine( test text 80 )");
    query = ret.transformQuery(query, queryParams);

    queryParams.set("working",
            Arrays.asList(new Long[]{0l, 1l, 2l, 3l, 4l, 89l, 90l, 91l, 92l, 93l}));
    results = model.executeQuery(query, queryParams);

    assertEquals(results.size(), 100);

    // higher documents, with the term '89', 
    // are ranked highest because 'test' and 'text' exist in every document (~= stopwords)
    assertEquals(results.get(0).document, 89);
    assertEquals(results.get(0).begin, 75);
    assertEquals(results.get(0).end, 85);
    assertEquals(results.get(0).rank, 1);
    assertEquals(results.get(0).score, -4.49422735, 0.000001);

    assertEquals(results.get(1).document, 89);
    assertEquals(results.get(1).begin, 80);
    assertEquals(results.get(1).end, 90);
    assertEquals(results.get(1).rank, 2);
    assertEquals(results.get(1).score, -4.49422735, 0.000001);

    assertEquals(results.get(10).document, 0);
    assertEquals(results.get(10).begin, 0);
    assertEquals(results.get(10).end, 10);
    assertEquals(results.get(10).rank, 11);
    assertEquals(results.get(10).score, -4.51151864, 0.000001);

    assertEquals(results.get(15).document, 89);
    assertEquals(results.get(15).begin, 0);
    assertEquals(results.get(15).end, 10);
    assertEquals(results.get(15).rank, 16);
    assertEquals(results.get(15).score, -4.51151864, 0.000001);

  }

  private void makeIndex(File corpus, File index) throws Exception {
    StringBuilder c = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      StringBuilder data = new StringBuilder();
      for (int j = 0; j < (i + 10); j++) {
        data.append(" ").append(j);
      }
      c.append(AppTest.trecDocument("d-" + i, "Test text" + data.toString()));
    }
    Utility.copyStringToFile(c.toString(), corpus);

    Parameters p = new Parameters();
    p.set("inputPath", corpus.getAbsolutePath());
    p.set("indexPath", index.getAbsolutePath());
    p.set("corpus", false);
    App.run("build", p, System.out);
  }
}
