/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.stats;

import java.io.Serializable;
import org.lemurproject.galago.utility.Parameters;

/**
 * Interface for all statistics.
 * 
 * @author sjh
 */
public abstract class AggregateStatistics implements Serializable {

  public abstract Parameters toParameters();

  public abstract void add(AggregateStatistics s);

  public String toString() {
    return toParameters().toString();
  }

}
