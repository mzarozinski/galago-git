/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.execution.Step;


/**
 * 
 * 
 * @author sjh
 */
public class TupleFlowUtility {
  
  /**
   * Builds a simple Sorter step that can be added to a TupleFlow stage.
   *
   * @param sortOrder An order object representing how and what to sort.
   * @return a Step object that can be added to a TupleFlow Stage.
   */
  public static Step getSorter(Order sortOrder) {
    return getSorter(sortOrder, null, CompressionType.VBYTE);
  }

  public static Step getSorter(Order sortOrder, CompressionType c) {
    return getSorter(sortOrder, null, c);
  }

  /**
   * Builds a Sorter step with a reducer that can be added to a TupleFlow stage.
   *
   * @param sortOrder An order object representing how and what to sort.
   * @param reducerClass The class of a reducer object that can reduce this
   * data.
   * @return a Step object that can be added to a TupleFlow Stage.
   */
  public static Step getSorter(Order sortOrder, Class reducerClass) {
    return getSorter(sortOrder, null, CompressionType.VBYTE);
  }

  public static Step getSorter(Order sortOrder, Class reducerClass, CompressionType c) {
    Parameters p = new Parameters();
    p.set("class", sortOrder.getOrderedClass().getName());
    p.set("order", org.lemurproject.galago.utility.Utility.join(sortOrder.getOrderSpec()));
    if (c != null) {
      p.set("compression", c.toString());
//      System.err.println("Setting sorter to :" + c.toString() + " -- " + join(sortOrder.getOrderSpec()));
//    } else {
//      System.err.println("NOT setting sorter to : null -- " + join(sortOrder.getOrderSpec()));
    }

    if (reducerClass != null) {
      try {
        reducerClass.asSubclass(Reducer.class);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException("getSorter called with a reducerClass argument "
                + "which is not actually a reducer: "
                + reducerClass.getName());
      }
      p.set("reducer", reducerClass.getName());
    }
    return new Step(Sorter.class, p);
  }
}
