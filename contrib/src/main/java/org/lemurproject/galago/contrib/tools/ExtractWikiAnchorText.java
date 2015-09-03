package org.lemurproject.galago.contrib.tools;

import java.io.PrintStream;
//importing org.lemurproject.galago.core.parse.LinkCombiner;
//importing org.lemurproject.galago.core.parse.LinkExtractor;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;

//
///**
// * 
// * @author jdalton
// */
public class ExtractWikiAnchorText extends AppFunction {
//    public Job getAnchorTextJob(Parameters buildParameters) throws IOException {
//
//	return new Job().add(BuildStageTemplates.getSplitStage(buildParameters.getAsList("inputPath"), 
//							       DocumentSource.class, buildParameters))
//	    .add(passThroughStage())
//	    .add(getParseLinksStage(buildParameters))
//	    .add(getLinkCombineStage())
//	    .connect("inputSplit", "parseLinks", ConnectionAssignmentType.Each)
//	    .connect("parseLinks", "passthrough", ConnectionAssignmentType.Combined)
//	    .connect("passthrough", "linkCombine", ConnectionAssignmentType.Combined)
//	    .connect("parseLinks", "linkCombine", ConnectionAssignmentType.Combined);
//    }
//	
//    public Stage getParseLinksStage(Parameters buildParameters) {
//	Stage stage = new Stage("parseLinks")
//	    .addInput("splits", new DocumentSplit.FileIdOrder())
//	    .addOutput("links", new ExtractedLink.DestUrlOrder())
//	    .addOutput("documentUrls", new NumberedDocumentData.UrlOrder())
//	    .add(new InputStep("splits"))
//	    .add(BuildStageTemplates.getParserStep(buildParameters))
//	    .add(BuildStageTemplates.getTokenizerStep(buildParameters))
//	    .add(new Step(DocumentNumberer.class));
//
//	MultiStep multi = new MultiStep();
//	ArrayList<Step> links =
//	    BuildStageTemplates.getExtractionSteps("links", LinkExtractor.class, new ExtractedLink.DestUrlOrder());
//	ArrayList<Step> data =
//	    BuildStageTemplates.getExtractionSteps("documentUrls", NumberedDocumentDataExtractor.class,
//						   new NumberedDocumentData.UrlOrder());
//	return stage.add(multi.addGroup(links).addGroup(data));
//    }
//	
//    public Stage passThroughStage() {
//	return new Stage("passthrough")
//	    .addInput("documentUrls", new NumberedDocumentData.UrlOrder())
//	    .addOutput("documentUrls2", new NumberedDocumentData.UrlOrder())		
//	    .add(new InputStep("documentUrls"))
//	    //.add(new Step(Identity.class))
//	    .add(new OutputStep("documentUrls2"));
//    }
//
//    public Stage getLinkCombineStage() {
//	Stage stage = new Stage("linkCombine")
//	    .addInput("documentUrls2", new NumberedDocumentData.UrlOrder())
//	    .addInput("links", new ExtractedLink.DestUrlOrder());
//
//	// Steps
//	Parameters p = new Parameters();
//	p.set("documentDatas", "documentUrls2");
//	p.set("extractedLinks", "links");
//	return stage.add(new Step(LinkCombiner.class, p))
//	    .add(new Step(AnchorTextCreator.class))
//	    .add(Utility.getSorter(new AdditionalDocumentText.IdentifierOrder()))
//	    .add(new Step(AnchorTextWriter.class, p));
//    }
//

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    output.println("Deprecated - use harvest-links.");
  }
    
//    // build-fast index input
//    if (!p.isList("inputPath")) {
//      output.println(getHelpString());
//      return;
//    }
//
//    Job job;
//    BuildIndex build = new BuildIndex();
//    job = build.getIndexJob(p);
//
//    if (job != null) {
//      runTupleFlowJob(job, p, output);
//    }
//}
//
//
//  public static void main(String[] args) throws Exception {
//	Parameters p = new Parameters();
//	p.set("inputPath", args[0]);
//	p.set("mode", "local");
//	p.set("tokenizer", new Parameters());
//	String[] fields = {"a"};
//	p.getMap("tokenizer").set("fields", Arrays.asList(fields));
//	ExtractWikiAnchorText textExtractor = new ExtractWikiAnchorText();
//	Job job = textExtractor.getAnchorTextJob(p);
//	runTupleFlowJob(job, p, System.err);
//  }
  @Override
  public String getName() {
    return "extract-wiki-anchor-text";
  }

  @Override
  public String getHelpString() {
    // TODO Auto-generated method stub
    return null;
  }
}
