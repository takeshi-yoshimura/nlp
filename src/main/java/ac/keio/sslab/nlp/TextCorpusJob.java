package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;
import ac.keio.sslab.nlp.corpus.DocumentFilter;
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
		Path outputPath = mgr.getJobIDPath(conf.corpusPath);
		String s = mgr.getArgStr("s");
		boolean tokenizeAtUnderline = mgr.getArgOrDefault("t", true, Boolean.class);
		boolean useNLTKStopwords = mgr.getArgOrDefault("n", false, Boolean.class);

		try {
			SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, conf.tmpPath, new Configuration(), mgr.doForceWrite(), String.class, String.class);
			SimpleTextCorpusReader reader = new SimpleTextCorpusReader(input, s);
			DocumentFilter filter = new DocumentFilter(tokenizeAtUnderline, useNLTKStopwords);
			StringBuilder sb = new StringBuilder();
			while(reader.seekNext()) {
				sb.setLength(0);
				for (String para: filter.filterDocument(reader.getDoc())) {
					sb.append(para).append(' ');
				}
				writer.append(reader.getId(), sb.toString());
			}
			writer.close();
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
