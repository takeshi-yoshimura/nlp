package ac.keio.sslab.nlp;

import java.io.File;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;
import ac.keio.sslab.nlp.corpus.DocumentFilter;
import ac.keio.sslab.nlp.corpus.SimpleTextCorpusReader;

public class TextCorpusJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();

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
		Options options = new Options();
		options.addOption("i", "input", true, "Input file");
		options.addOption("s", "separator", true, "separator of ID and data sections");
		options.addOption("t", "tokenizeAtUnderline", true, "tokenize at underline? (default is true)");
		options.addOption("n", "useNLTKStopwords", true, "use NLTK stopwords? (default is false)");
		return options;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("i") || !args.containsKey("s")) {
			System.err.println("Need to specify -i, -s");
			return;
		}
		File input = new File(args.get("i"));
		Path outputPath = new Path(conf.corpusPath, args.get("j"));
		String s = args.get("s");
		boolean tokenizeAtUnderline = true;
		if (args.containsKey("t")) {
			tokenizeAtUnderline = Boolean.parseBoolean(args.get("t"));
		}
		boolean useNLTKStopwords = false;
		if (args.containsKey("n")) {
			useNLTKStopwords = Boolean.parseBoolean(args.get("n"));
		}

		try {
			SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, conf.tmpPath, new Configuration(), args.containsKey("ow"));
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
