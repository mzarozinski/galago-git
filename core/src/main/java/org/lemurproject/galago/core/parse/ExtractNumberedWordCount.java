/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Utility;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberWordCount")
public class ExtractNumberedWordCount extends StandardStep<Document, NumberWordCount> {

  @Override
  public void process(Document d) throws IOException {
    for (int i = 0; i < d.terms.size(); i++) {
      processor.process(new NumberWordCount(Utility.fromString(d.terms.get(i)), d.identifier, 1));
    }
  }
}
