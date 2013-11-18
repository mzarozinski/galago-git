// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.stats.AggregateStatistics;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.utility.Parameters;

/**
 * <p>
 * This is a base interface for all kinds of retrieval classes. Historically
 * this was used to support binned indexes in addition to structured
 * indexes.</p>
 *
 * This interface now defines the basic functionality every Retrieval
 * implementation should have.
 *
 * @author trevor
 * @author irmarc
 */
public interface Retrieval {

  /**
   * Should close the Retrieval and release any underlying resources.
   *
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * Returns the Parameters object that parameterize the retrieval object, if
   * one exists.
   */
  public Parameters getGlobalParameters();

  /**
   * Returns the index parts available under this retrieval. The parts are
   * returned as a Parameters object, which acts as a map between parts and the
   * node types they support.
   *
   * @return
   * @throws IOException
   */
  public Parameters getAvailableParts() throws IOException;

  /**
   * Attempts to return a NodeType object for the supplied Node.
   *
   * @param node
   * @return
   * @throws Exception
   */
  public NodeType getNodeType(Node node) throws Exception;

  /**
   * Attempts to return a QueryType object for the supplied Node. This is
   * typically called on root nodes of query trees. It allows a semi-automatic
   * selection of processing model.
   *
   * @param node
   * @return
   * @throws Exception
   */
  public QueryType getQueryType(Node node) throws Exception;

  /**
   * Performs any additional transformations necessary to prepare the query for
   * execution.
   *
   * @param root
   * @param queryParams a Parameters object that may further populated by the
   * transformations applied
   * @return
   * @throws Exception
   */
  public Node transformQuery(Node root, Parameters queryParams) throws Exception;

  /**
   * Runs the query against the retrieval. Assumes the query has been properly
   * annotated. An example is the query produced from transformQuery.
   *
   * @param root
   * @return Results (contains a list of scored documents)
   * @throws Exception
   */
  public Results executeQuery(Node root) throws Exception;

  /**
   * Runs the query against the retrieval. Assumes the query has been properly
   * annotated. An example is the query produced from transformQuery. Parameters
   * object allows any global execution parameters or default values to be
   * overridden.
   *
   * @param root, parameters
   * @return Results (contains a list of scored documents)
   * @throws Exception
   */
  public Results executeQuery(Node root, Parameters parameters) throws Exception;

  /**
   * Returns some type of statistics, depending on the parameters and the node
   * specified.
   *
   * Three forms are currently supported: index-part-statistics,
   * collection-statistics, and node-statistics. See retrieval.stats for more
   * details.
   *
   * @param root
   * @param parameters
   * @return stats
   * @throws IOException
   */
  public AggregateStatistics getStatisics(Node root, Parameters parameters) throws Exception;

  /**
   * Returns a set of the same type of statistics, one for each node, depending
   * on the parameters and the node specified.
   *
   * Three forms are currently supported: index-part-statistics,
   * collection-statistics, and node-statistics. See retrieval.stats for more
   * details.
   *
   * @param nodes
   * @param parameters
   * @return Map<Node, AggregateStatistics>
   * @throws Exception
   */
  public Map<Node, AggregateStatistics> getStatisics(Collection<Node> nodes, Parameters parameters) throws Exception;

  /**
   * Returns the length of a particular document. Where docid is the internal
   * identifier of the document.
   *
   * @param docid
   * @return document length
   * @throws IOException
   */
  public Integer getDocumentLength(Integer docid) throws IOException;

  /**
   * Returns the length of a particular document. Where docname is the
   * internally stored name of the document.
   *
   * @param docname
   * @return document length
   * @throws IOException
   */
  public Integer getDocumentLength(String docname) throws IOException;

  /**
   * Returns the internally stored name of a particular document. Where docid is
   * the internal identifier of the document.
   *
   * @param docid
   * @return document length
   * @throws IOException
   */
  public String getDocumentName(Integer docid) throws IOException;

  /**
   * Returns the requested Document, if found.
   *
   * @param identifier The external name of the document to locate.
   * @return If found, the Document object. Null otherwise.
   * @throws IOException
   */
  public Document getDocument(String identifier, DocumentComponents p) throws IOException;

  /**
   * Returns a Map of Document objects that have been found, given the list of
   * identifiers provided.
   *
   * @param identifier
   * @return
   * @throws IOException
   */
  public Map<String, Document> getDocuments(List<String> identifier, DocumentComponents p) throws IOException;

  /**
   * Returns IndexPartStatistics for the named postings part.
   *
   * Data includes statistics for vocabulary size, total number of postings
   * stored and longest posting list.
   *
   * @deprecated
   * @param partName
   * @return IndexPartStatistics
   * @throws IOException
   */
  public IndexPartStatistics getIndexPartStatistics(String partName) throws IOException;

  /**
   * Returns statistics for a string representation of a lengths node. See
   * collectionStatistics(Node node).
   *
   * Data returned includes collectionLength, document count, longest document,
   * shortest document, average document.
   *
   * @deprecated
   * @param nodeString
   * @return FieldStatistics
   * @throws Exception
   */
  public FieldStatistics getCollectionStatistics(String nodeString) throws Exception;

  /**
   * Returns statistics for a lengths node. This data is commonly used in
   * probabilistic smoothing functions.
   *
   * The root-node must implement LengthsIterator.
   *
   * Data returned includes collectionLength, document count, longest document,
   * shortest document, average document. Where 'document' may be a 'field' or
   * other specified region of indexed documents.
   *
   * @deprecated
   * @param node
   * @return FieldStatistics
   * @throws Exception
   */
  public FieldStatistics getCollectionStatistics(Node node) throws Exception;

  /**
   * Returns collection statistics for a count node. This data is commonly used
   * as a feature in a retrieval model. See nodeStatistics(Node node).
   *
   * Data returned includes the frequency of the node in the collection, the
   * number of documents that return a non-zero count for the node, and the
   * maximum frequency of the node in any single document.
   *
   * @deprecated
   * @param nodeString
   * @return NodeStatistics
   * @throws Exception
   */
  public NodeStatistics getNodeStatistics(String nodeString) throws Exception;

  /**
   * Returns collection statistics for a count node. This data is commonly used
   * as a feature in a retrieval model.
   *
   * The root-node must implement a 'CountIterator'.
   *
   * Data returned includes the frequency of the node in the collection, the
   * number of documents that return a non-zero count for the node, and the
   * maximum frequency of the node in any single document.
   *
   * @deprecated
   * @param node
   * @return NodeStatistics
   * @throws Exception
   */
  public NodeStatistics getNodeStatistics(Node node) throws Exception;

  /**
   * adds a node to the cache -- can improve efficiency for repeated queries --
   * if no cache is present, function does nothing
   */
  public void addNodeToCache(Node node) throws Exception;

  /**
   * recursively adds nodes to the cache -- all children nodes in the tree are
   * added to the cache -- can improve efficiency for repeated queries -- if no
   * cache is present, function does nothing
   */
  public void addAllNodesToCache(Node node) throws Exception;
}
