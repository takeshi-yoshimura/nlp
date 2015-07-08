package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

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
		
		MyAnalyzer analyzer = new MyAnalyzer();
		Text key = new Text(); Text value = new Text();

		SequenceFile.Writer writer = null;
		try {
			Configuration hdfsConf = new Configuration();
			FileSystem fs = FileSystem.get(hdfsConf);
			if (fs.exists(outputPath)) {
				if (args.containsKey("ow")) {
					if (!JobUtils.promptDeleteDirectory(fs, outputPath, args.containsKey("force"))) {
						throw new Exception("Overwrite revoked");
					}
				} else {
					throw new Exception(outputPath.toString() + " exists. You might want to use --overwrite.");
				}
			}
			Path tmpOutputPath = new Path(conf.tmpPath, outputPath.toString());
			SequenceFile.Writer.Option fileOpt = SequenceFile.Writer.file(tmpOutputPath);
			SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(Text.class);
			SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(Text.class);
			fs.mkdirs(tmpOutputPath.getParent());
			writer = SequenceFile.createWriter(hdfsConf, fileOpt, keyClass, valueClass);
			
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
							filtered.append(word.toLowerCase());
							filtered.append(GitCorpusJob.delimiter);
				        }
				        stream.end();
				        stream.close();
						if (filtered.length() >= GitCorpusJob.delimiter.length()) {
							//strip the last delimiter
							filtered.setLength(filtered.length() - GitCorpusJob.delimiter.length());
							//do not touch key
							value.set(filtered.toString());
							writer.append(key, value);
						}
						dataStr.setLength(0);
						idStr.setLength(0);
					}
					inData = false;
				} else if (line == dS) {
					inID = false;
					inData = true;
					key.set(idStr.toString());
					System.err.println("Writing id " + idStr.toString());
				} else if (inID) {
					idStr.append(line).append('\n');
				} else if (inData) {
					dataStr.append(line).append('\n');
				}
			}
			br.close();
			writer.close();

			fs.mkdirs(outputPath.getParent());
			fs.rename(tmpOutputPath, outputPath);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to load Local file " + input.getAbsolutePath());
			System.err.println("OR Failed to write HDFS file " + new Path(conf.tmpPath, outputPath.toString()) + " or " + outputPath);
		}
		analyzer.close();
	}

	@Override
	public void takeSnapshot() {
	}

	@Override
	public void restoreSnapshot() {
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
