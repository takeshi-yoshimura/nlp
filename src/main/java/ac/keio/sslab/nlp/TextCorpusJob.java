package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;

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
		options.addOption("iS", "ID_separator", true, "begining separator of ID sections");
		options.addOption("dS", "data_separator", true, "begin separator of data sections");
		options.addOption("t", "tokenizeAtUnderline", true, "tokenize at underline? (default is true)");
		options.addOption("n", "useNLTKStopwords", true, "use NLTK stopwords? (default is false)");
		return options;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("i") || !args.containsKey("iS") || !args.containsKey("dS")) {
			System.err.println("Need to specify -i, -iS, -dS");
			return;
		}
		File input = new File(args.get("i"));
		Path outputPath = new Path(conf.corpusPath, args.get("j"));
		String iS = args.get("iS");
		String dS = args.get("dS");
		boolean tokenizeAtUnderline = true;
		if (args.containsKey("t")) {
			tokenizeAtUnderline = Boolean.parseBoolean(args.get("t"));
		}
		boolean useNLTKStopwords = false;
		if (args.containsKey("n")) {
			useNLTKStopwords = Boolean.parseBoolean(args.get("n"));
		}
		
		MyAnalyzer analyzer = new MyAnalyzer(tokenizeAtUnderline, useNLTKStopwords);
		String key = null;
		try {
			SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, conf.tmpPath, new Configuration(), args.containsKey("ow"));
			
			BufferedReader br = new BufferedReader(new FileReader(input));
			String line;
			boolean inID = false;
			boolean inData = false;
			StringBuilder idStr = new StringBuilder();
			StringBuilder dataStr = new StringBuilder();
			while ((line = br.readLine()) != null) {
				if (line == iS) {
					inID = true;
					if (inData) {
						StringBuilder preprocessed = new StringBuilder();
						// TODO: the following ignores messages in a single paragraph that includes signed-off-by. Should consider the case?
						// TODO: use Lucene/Solr to process both paragraphs and words
						for (String para: dataStr.toString().split("\n\n")) {
							if (para.toLowerCase().indexOf("signed-off-by:") != -1 || para.toLowerCase().indexOf("cc:") != -1) {
								continue;
							}
							preprocessed.append(para);
							preprocessed.append(' ');
						}

						StringBuilder filtered = new StringBuilder();
						Reader reader = new StringReader(preprocessed.toString());
				        TokenStream stream = analyzer.tokenStream("", reader);
			            CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
			            stream.reset();

				        while (stream.incrementToken()) {
				        	String word = term.toString();
							if (word.matches("[0-9]+")) {
								continue;
							} else if ((word.matches("[a-f0-9]+") && !word.matches("[a-f]+")) || (word.matches("0x[a-f0-9]+"))) {
								continue;
							}
							filtered.append(word);
							filtered.append(GitCorpusJob.delimiter);
				        }
				        stream.end();
				        stream.close();
						if (filtered.length() >= GitCorpusJob.delimiter.length()) {
							//strip the last delimiter
							filtered.setLength(filtered.length() - GitCorpusJob.delimiter.length());
							//do not touch key
							writer.append(key, filtered.toString());
						}
						dataStr.setLength(0);
						idStr.setLength(0);
					}
					inData = false;
				} else if (line == dS) {
					inID = false;
					inData = true;
					key = idStr.toString();
					System.err.println("Writing id " + idStr.toString());
				} else if (inID) {
					idStr.append(line).append('\n');
				} else if (inData) {
					dataStr.append(line).append('\n');
				}
			}
			br.close();
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to load Local file " + input.getAbsolutePath());
			System.err.println("OR Failed to write HDFS file " + new Path(conf.tmpPath, outputPath.toString()) + " or " + outputPath);
		}
		analyzer.close();
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
