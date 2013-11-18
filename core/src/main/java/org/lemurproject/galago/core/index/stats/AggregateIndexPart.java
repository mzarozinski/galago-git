/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.stats;

/**
 * Interface to allow some IndexPartReader classes to provide some aggregate statistics.
 *  
 * @author sjh
 */
public interface AggregateIndexPart {

  public IndexPartStatistics getStatistics();
}
