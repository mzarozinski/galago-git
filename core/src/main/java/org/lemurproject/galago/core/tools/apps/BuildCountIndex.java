/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import java.util.List;
import org.lemurproject.galago.core.index.ExtractIndexDocumentNumbers;
import org.lemurproject.galago.core.index.disk2.CountIndexWriter;
import org.lemurproject.galago.core.parse.ExtractNumberedWordCount;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.parse.stem.NullStemmer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.tools.AppFunction;
import static org.lemurproject.galago.core.tools.AppFunction.getTupleFlowParameterString;
import static org.lemurproject.galago.core.tools.AppFunction.runTupleFlowJob;
import org.lemurproject.galago.core.tools.apps.BuildStageTemplates;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.core.window.ReduceNumberWordCount;
import org.lemurproject.galago.tupleflow.TupleFlowUtility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.ConnectionPointType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.StageConnectionPoint;
import org.lemurproject.galago.tupleflow.execution.Step;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author sjh
 */
public class BuildCountIndex extends AppFunction {

  String indexPath;
  boolean stemming;
  Parameters buildParameters;
  String stemmerName;
  Class stemmerClass;
  String compression;

  public Stage getParsePostingsStage(String stageName, String inputStream, String outputStream) throws Exception {
    // reads through the corpus
    Stage stage = new Stage(stageName);

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input,
            inputStream, new DocumentSplit.FileIdOrder()));

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Output,
            outputStream, new NumberWordCount.WordDocumentOrder()));

    stage.add(new InputStep(inputStream));
    stage.add(BuildStageTemplates.getParserStep(buildParameters));
    stage.add(BuildStageTemplates.getTokenizerStep(buildParameters));
    if (stemming) {
      Class stemmer = stemmerClass;
      stage.add(BuildStageTemplates.getStemmerStep(new Parameters(), stemmer));
    }

    Parameters p = new Parameters();
    p.set("indexPath", indexPath);
    stage.add(new Step(ExtractIndexDocumentNumbers.class, p));

    stage.add(new Step(ExtractNumberedWordCount.class));
    stage.add(TupleFlowUtility.getSorter(new NumberWordCount.WordDocumentOrder()));
    stage.add(new Step(ReduceNumberWordCount.class));

    stage.add(new OutputStep(outputStream));
    return stage;
  }

  public Stage getWritePostingsStage(String stageName, String inputName, String indexName) {
    Stage stage = new Stage(stageName);

    stage.add(new StageConnectionPoint(
            ConnectionPointType.Input,
            inputName, new NumberWordCount.WordDocumentOrder()));

    stage.add(new InputStep(inputName));

    // accumulate counts
    stage.add(new Step(ReduceNumberWordCount.class));

    Parameters p2 = new Parameters();
    p2.set("filename", indexName);
    p2.set("compression", compression);
    if (stemming) {
      p2.set("stemming", stemming); // slightly redundent only present if true //
      p2.set("stemmer", stemmerClass.getName());
    }

    stage.add(new Step(CountIndexWriter.class, p2));
    return stage;
  }

  public Job getIndexJob(Parameters p) throws Exception {

    Job job = new Job();
    this.buildParameters = p;

    List<String> inputPaths = p.getAsList("inputPath");

    // application of defaulty values
    this.stemming = p.get("stemming", true);
    if (stemming) {
      if (p.isString("stemmer") && p.isString("stemmerClass")) {
        stemmerName = p.getString("stemmer");
        stemmerClass = Class.forName(p.getString("stemmerClass"));
      } else if (p.isString("stemmer")) {
        stemmerName = p.getString("stemmer");
        stemmerClass = null;
        if (stemmerName.equals("null")) {
          stemmerClass = NullStemmer.class;
        } else if (stemmerName.equals("porter")) {
          stemmerClass = Porter2Stemmer.class;
        } else if (stemmerName.equals("krovetz")) {
          stemmerClass = KrovetzStemmer.class;
        } else {
          throw new RuntimeException("A stemmerClass must be specified for stemmer " + stemmerName);
        }
      } else if (p.isString("stemmerClass")) {
        stemmerClass = Class.forName(p.getString("stemmerClass"));
        stemmerName = p.getString("stemmerClass").replaceFirst(".*\\.", "");
      } else {
        // defaults:
        stemmerName = "krovetz";
        stemmerClass = KrovetzStemmer.class;
      }
    }

    this.indexPath = p.getString("indexPath");
    this.compression = p.get("compression", "vbyte");

    // tokenizer - fields
    if (buildParameters.isList("fields", Parameters.Type.STRING) || buildParameters.isString("fields")) {
      buildParameters.set("tokenizer", new Parameters());
      buildParameters.getMap("tokenizer").set("fields", buildParameters.getAsList("fields"));
    }

    String indexName = p.getString("outputIndexName");

    Parameters splitParameters = new Parameters();
    splitParameters.set("corpusPieces", p.get("distrib", 10));
    job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, new DocumentSplit.FileIdOrder(), splitParameters));
    job.add(getParsePostingsStage("parsePostings", "splits", "counts"));
    job.add(getWritePostingsStage("writePostings", "counts", indexName));

    job.connect("inputSplit", "parsePostings", ConnectionAssignmentType.Each);
    job.connect("parsePostings", "writePostings", ConnectionAssignmentType.Combined);

    return job;
  }

  @Override
  public String getName() {
    return "build-count-index";
  }

  @Override
  public String getHelpString() {
    return "galago build-window [flags] --indexPath=<index> (--inputPath+<input>)+\n\n"
            + "  Builds a Galago StructuredIndex window index file using TupleFlow. Program\n"
            + "  uses one thread for each CPU core on your computer.  While some debugging output\n"
            + "  will be displayed on the screen, most of the status information will\n"
            + "  appear on a web page.  A URL should appear in the command output \n"
            + "  that will direct you to the status page.\n\n"
            + "  Arg: --spaceEfficient=true will produce an identical window index using "
            + "  a two-pass space efficient algorithm. \n\n"
            + "  Ordered or unordered windows can be generated. We match the #od and\n"
            + "  #uw operator definitions (See galago query language). Width of an ordered window\n"
            + "  is the maximum distance between words. Width of an unordered window is\n"
            + "  the differencebetween the location of the last word and the location of \n"
            + "  the first word.\n\n"
            + "  <input>:  Can be either a file or directory, and as many can be\n"
            + "          specified as you like.  Galago can read html, xml, txt, \n"
            + "          arc (Heritrix), trectext, trecweb and corpus files.\n"
            + "          Files may be gzip compressed (.gz).\n"
            + "  <index>:  The directory path of the existing index (over the same corpus).\n\n"
            + "Algorithm Flags:\n"
            + "  --n={int >= 2}:          Selects the number of terms in each window (any reasonable value is possible).\n"
            + "                           [default = 2]\n"
            + "  --width={int >= 1}:      Selects the width of the window (Note: ordered windows are different to unordered windows).\n"
            + "                           [default = 1]\n"
            + "  --ordered={true|false}:  Selects ordered or unordered windows.\n"
            + "                           [default = true]\n"
            + "  --threshold={int >= 1}:  Selects the minimum number length of any inverted list.\n"
            + "                           Larger values will produce smaller indexes.\n"
            + "                           [default = 2]\n"
            + "  --usedocfreq={true|false}: Determines if the threshold is applied to term freq or doc freq.\n"
            + "                           [default = false]\n"
            + "  --stemming={true|false}: Selects to stem terms with which to build a stemmed ngram inverted list.\n"
            + "                           [default=true]\n"
            + "  --fields+{field-name}:   Selects field parts to index.\n"
            + "                           [omitted]\n"
            + "  --spaceEfficient={true|false}: Selects whether to use a space efficient algorithm.\n"
            + "                           (The cost is an extra pass over the input data).\n"
            + "                           [default=false]\n"
            + "  --positionalIndex={true|false}: Selects whether to write positional data to the index file.\n"
            + "                           (The benefit is a large decrease in space usage).\n"
            + "                           [default=true]\n\n"
            + getTupleFlowParameterString();
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {

    Job job;
    BuildCountIndex build = new BuildCountIndex();
    job = build.getIndexJob(p);

    runTupleFlowJob(job, p, output);
  }

  public static void main(String[] args) throws Exception {
    new BuildCountIndex().run(Parameters.parseArgs(args), System.out);
  }

}
