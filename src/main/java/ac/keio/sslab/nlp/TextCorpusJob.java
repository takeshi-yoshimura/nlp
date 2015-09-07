package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.corpus.PatchCorpusWriter;
import ac.keio.sslab.nlp.corpus.SimpleTextCorpusReader;

public class TextCorpusJob implements NLPJob {

	@Override
	public String getJobName() {
		return "textCorpus";
	}

	@Override
	public String getJobDescription() {
		return "create corpus from a text file (format is defined)";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("i", "input", true, "Input file"));
		g.addOption(new Option("s", "separator", true, "separator of ID and data sections"));
		g.setRequired(true);

		Options options = new Options();
		options.addOptionGroup(g);
		options.addOption("t", "tokenizeAtUnderline", true, "tokenize at underline? (default is true)");
		options.addOption("n", "useNLTKStopwords", true, "use NLTK stopwords? (default is false)");
		return options;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		File input = new File(mgr.getArgStr("i"));
		File outputDir = new File(conf.localCorpusFile, mgr.getArgStr("j"));
		Path outputPath = mgr.getJobIDPath(conf.corpusPath);
		String s = mgr.getArgStr("s");
		boolean splitParagraph = mgr.getArgOrDefault("p", false, Boolean.class);
		boolean tokenizeAtUnderline = mgr.getArgOrDefault("t", true, Boolean.class);
		boolean useNLTKStopwords = mgr.getArgOrDefault("n", false, Boolean.class);

		try {
			PatchCorpusWriter writer = new PatchCorpusWriter(outputDir, mgr.doForceWrite(), splitParagraph, tokenizeAtUnderline, useNLTKStopwords);
			SimpleTextCorpusReader reader = new SimpleTextCorpusReader(input, s);
			while(reader.seekNext()) {
				System.out.println("write " + reader.getID());
				writer.processPatchMessage(reader.getID(), reader.getDate(), reader.getVersion(), reader.getFiles(), reader.getDoc());
			}
			writer.emitSummary(conf.hdfs, outputPath, conf.tmpPath, reader.getStats());
			writer.close();
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to load Local file " + input.getAbsolutePath());
			System.err.println("OR Failed to write HDFS file " + new Path(conf.tmpPath, outputPath.toString()) + " or " + outputPath);
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
