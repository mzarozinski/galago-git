/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal.optimize;

import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class FlattenWindowTraversalTest extends TestCase {

  public FlattenWindowTraversalTest(String testName) {
    super(testName);
  }

  public void testNestedWindowRewrite() throws Exception {
    String query = "#uw:5( #od:1(#text:a() #text:b()) )";
    Node result = StructuredQuery.parse(query);
    Node transformed = new FlattenWindowTraversal().traverse(result, new Parameters());
    assertEquals("#od:1( #text:a() #text:b() )", transformed.toString());
  }
}
