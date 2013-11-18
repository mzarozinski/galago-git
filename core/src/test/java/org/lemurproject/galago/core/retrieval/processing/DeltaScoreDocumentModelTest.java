/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.io.File;
import java.util.List;
import static junit.framework.Assert.assertEquals;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
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
public class DeltaScoreDocumentModelTest extends TestCase {

  public DeltaScoreDocumentModelTest(String testName) {
    super(testName);
  }

  public void testMaxscore() throws Exception {
    File corpus = Utility.createTemporary();
    File index = Utility.createTemporaryDirectory();
    try {
      makeIndex(corpus, index);

      Parameters globals = new Parameters();
      LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

      Parameters queryParams = new Parameters();
      queryParams.set("requested", 10);

      Node query = StructuredQuery.parse("#combine( test text 0 1 2 3 4 )");
      query = ret.transformQuery(query, queryParams);

      MaxScoreDocumentModel deltaModel = new MaxScoreDocumentModel(ret);
      List<ScoredDocument> deltaResults = deltaModel.executeQuery(query, queryParams);

      RankedDocumentModel safeModel = new RankedDocumentModel(ret);
      List<ScoredDocument> safeResults = safeModel.executeQuery(query, queryParams);

      assertEquals(safeResults.size(), deltaResults.size());
      for (int i = 0; i < safeResults.size(); ++i) {
        assertEquals(safeResults.get(i).document, deltaResults.get(i).document);
        assertEquals(safeResults.get(i).score, deltaResults.get(i).score, 0.00001);
      }

      // check that weights are correctly propagated
      queryParams.set("flattenCombine", false);
      Node complexQuery = StructuredQuery.parse("#combine:0=0.9:1=0.1(#combine( test text ) #combine( 1 2 ))");
      complexQuery = ret.transformQuery(complexQuery, queryParams);

      List<ScoredDocument> deltaResults2 = deltaModel.executeQuery(complexQuery, queryParams);
      List<ScoredDocument> safeResults2 = safeModel.executeQuery(complexQuery, queryParams);

      for (int i = 0; i < safeResults2.size(); ++i) {
        assertEquals(safeResults2.get(i).document, deltaResults2.get(i).document);
        assertEquals(safeResults2.get(i).score, deltaResults2.get(i).score, 0.00001);
      }

    } finally {
      corpus.delete();
      Utility.deleteDirectory(index);
    }
  }

  public void testWAND() throws Exception {
    File corpus = Utility.createTemporary();
    File index = Utility.createTemporaryDirectory();
    try {
      makeIndex(corpus, index);

      Parameters globals = new Parameters();
      LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

      Parameters queryParams = new Parameters();
      queryParams.set("requested", 10);

      Node query = StructuredQuery.parse("#combine( test text 0 1 2 3 4 90 )");
      query = ret.transformQuery(query, queryParams);

      WeakAndDocumentModel deltaModel = new WeakAndDocumentModel(ret);
      List<ScoredDocument> deltaResults = deltaModel.executeQuery(query, queryParams);

      RankedDocumentModel safeModel = new RankedDocumentModel(ret);
      List<ScoredDocument> safeResults = safeModel.executeQuery(query, queryParams);

      assertEquals(safeResults.size(), deltaResults.size());
      for (int i = 0; i < safeResults.size(); ++i) {
        assertEquals(safeResults.get(i).document, deltaResults.get(i).document);
        assertEquals(safeResults.get(i).score, deltaResults.get(i).score, 0.00001);
      }

      // check that weights are correctly propagated
      queryParams.set("flattenCombine", false);
      Node complexQuery = StructuredQuery.parse("#combine:0=0.9:1=0.1(#combine( test text ) #combine( 1 2 ))");
      complexQuery = ret.transformQuery(complexQuery, queryParams);

      List<ScoredDocument> deltaResults2 = deltaModel.executeQuery(complexQuery, queryParams);
      List<ScoredDocument> safeResults2 = safeModel.executeQuery(complexQuery, queryParams);

      for (int i = 0; i < safeResults2.size(); ++i) {
        assertEquals(safeResults2.get(i).document, deltaResults2.get(i).document);
        assertEquals(safeResults2.get(i).score, deltaResults2.get(i).score, 0.00001);
      }

    } finally {
      corpus.delete();
      Utility.deleteDirectory(index);
    }
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
    App.run("build", p, System.out);
  }
}
