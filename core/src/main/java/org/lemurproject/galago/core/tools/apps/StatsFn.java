/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import java.util.List;
import java.util.Set;
import org.lemurproject.galago.core.index.stats.AggregateStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class StatsFn extends AppFunction {

  @Override
  public String getName() {
    return "stats";
  }

  @Override
  public String getHelpString() {
    return "galago stats --index=/path/to/index [options]\n\n"
            + "\t--part+[partName]\n"
            + "\t--field+[fieldName]\n"
            + "\t--node+[countableNode]\n"
            + "\n\n"
            + "If no options are specified, output will be the part statistics"
            + "for the default postings part.\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("index")) {
      output.print(getHelpString());
      return;
    }

    Retrieval r = RetrievalFactory.instance(p);

    // TODO: this should be in the retrieval interface...
    if (!p.containsKey("part")
            && !p.containsKey("field")
            && !p.containsKey("node")) {
      Set<String> available = r.getAvailableParts().getKeys();
      if (available.contains("postings.krovetz")) {
        p.set("part", "postings.krovetz");
      } else if (available.contains("postings.porter")) {
        p.set("part", "postings.porter");
      } else if (available.contains("postings")) {
        p.set("part", "postings");
      } else {
        output.print(getHelpString());
        output.println("Could not determine default part.");
        return;
      }
    }

    Parameters o = new Parameters();

    Parameters p1 = Parameters.singleKeyValue("statCollector", "partStats");
    Parameters p2 = Parameters.singleKeyValue("statCollector", "collStats");
    Parameters p3 = Parameters.singleKeyValue("statCollector", "nodeStats");

    for (String part : (List<String>) p.getAsList("part")) {

      Node partNode = new Node("text", part);

      try {
        AggregateStatistics s = r.getStatisics(partNode, p1);
        o.set(part, s.toParameters());

      } catch (IllegalArgumentException e) {
        System.err.println(e.toString());
      }
    }
    for (String field : (List<String>) p.getAsList("field")) {

      Node fieldNode = StructuredQuery.parse(field);
      // Currently - I'm only willing to fix one type of lengths node:
      if (fieldNode.getOperator().equals("text")) {
        fieldNode.setOperator("lengths");
        fieldNode.getNodeParameters().set("part", "lengths");
      }

      try {
        AggregateStatistics s = r.getStatisics(fieldNode, p2);
        o.set(field, s.toParameters());

      } catch (IllegalArgumentException e) {
        System.err.println(e.toString());
      }
    }
    for (String node : (List<String>) p.getAsList("node")) {

      Node countNode = StructuredQuery.parse(node);
      // ensure the traversals don't wrap this count node //
      countNode.getNodeParameters().set("queryType", "count");
      countNode = r.transformQuery(countNode, new Parameters());

      try {
        AggregateStatistics s = r.getStatisics(countNode, p3);
        o.set(node, s.toParameters());
      } catch (IllegalArgumentException e) {
        System.err.println(e.toString());
      }
    }

    output.println(o.toPrettyString());
  }
}
