/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class ReplaceOperatorTraversalTest extends TestCase {
  
  public ReplaceOperatorTraversalTest(String testName) {
    super(testName);
  }

  public void testOpRepls() throws Exception {
    FakeRetrieval ret = new FakeRetrieval();
    Parameters repls = Parameters.parseArgs(new String[]{
      "--opRepls/dummy1=test1", 
      "--opRepls/dummy2+test2", 
      "--opRepls/dummy2+test3"});
    
    ReplaceOperatorTraversal traversal = new ReplaceOperatorTraversal(ret);
    
    Node t1 = traversal.afterNode(new Node("dummy1"), repls);
    Node t2 = traversal.afterNode(new Node("dummy2"), repls);
    
    System.err.println(t1.toString());
    System.err.println(t2.toString());

    assertEquals(t1.getOperator(), "test1");
    assertEquals(t2.getOperator(), "test2");
    assertEquals(t2.getChild(0).getOperator(), "test3");
    
  }
}
