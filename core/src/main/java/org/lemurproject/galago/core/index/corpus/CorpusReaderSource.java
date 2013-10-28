// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeKeySource;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.parse.Tokenizer;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author jfoley
 */
public class CorpusReaderSource extends BTreeKeySource implements DataSource<Document> {

  DocumentComponents docParams;
  Tokenizer tokenizer;

  public CorpusReaderSource(BTreeReader rdr) throws IOException {
    super(rdr);
    docParams = new DocumentComponents();
    final Parameters manifest = btreeReader.getManifest();

    tokenizer = Tokenizer.instance(manifest);
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public String key() {
    return "corpus";
  }

  @Override
  public boolean hasMatch(long id) {
    return (!isDone() && currentCandidate() == id);
  }

  @Override
  public Document data(long id) {
    if (currentCandidate() == id) {
      try {
        Document doc = Document.deserialize(btreeIter.getValueBytes(), docParams);
        if (docParams.tokenize) {
          tokenizer.tokenize(doc);
        }
        return doc;
      } catch (IOException ex) {
        Logger.getLogger(CorpusReaderSource.class.getName()).log(Level.SEVERE, "Failed to deserialize document " + id, ex);
      }
    }
    return null;
  }
}
