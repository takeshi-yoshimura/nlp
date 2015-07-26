package ac.keio.sslab.nlp;

import java.io.File;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;
import ac.keio.sslab.nlp.corpus.DocumentFilter;
import ac.keio.sslab.nlp.corpus.GitCorpusReader;
import ac.keio.sslab.nlp.corpus.GitLogCorpusReader;
import ac.keio.sslab.nlp.corpus.ShaFileGitCorpusReader;
import ac.keio.sslab.nlp.corpus.StableLinuxGitCorpusReader;

public class GitCorpusJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();

	@Override
	public String getJobName() {
		return "gitCorpus";
	}

	@Override
	public String getJobDescription() {
		return "Filter and upload logs in the specified git repository with the specified ref.";
	}

	@Override
	public Options getOptions() {
		Options options = new Options();
		options.addOption("g", "gitDir", true, "path to a git repository");
		options.addOption("s", "since", true, "Start object ref to be uploaded (yyyy/MM/dd). Default is blank (means all)");
		options.addOption("u", "until", true, "End object ref to be uploaded (yyyy/MM/dd). Default is HEAD.");
		options.addOption("f", "file", true, "target file or directory path in git repository. Default is the top of the input directory.");
		options.addOption("sl", "stableLinux", false, "Get all the commits for stable Linux (if specified, ignore -s and -u)");
		options.addOption("c", "commitFile", true, "File for commits to be extracted");
		options.addOption("t", "tokenizeAtUnderline", true, "tokenize at underline? (default is true)");
		options.addOption("n", "useNLTKStopwords", true, "use NLTK stopwords? (default is false)");
		options.addOption("p", "splitParagraph", true, "split paragraphs? (default is false)");
		return options;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("g")) {
			System.err.println("Need to specify --gitDir");
			return;
		}
		File inputDir = new File(args.get("g"));
		Path outputPath = new Path(conf.corpusPath, args.get("j"));
		String sinceStr = "";
		if (args.containsKey("s")) {
			sinceStr = args.get("s");
		}
		String untilStr = "HEAD";
		if (args.containsKey("u")) {
			untilStr = args.get("u");
		}
		String fileStr = null;
		if (args.containsKey("f")) {
			fileStr = args.get("f");
		}
		boolean tokenizeAtUnderline = true;
		if (args.containsKey("t")) {
			tokenizeAtUnderline = Boolean.parseBoolean(args.get("t"));
		}
		boolean useNLTKStopwords = false;
		if (args.containsKey("n")) {
			useNLTKStopwords = Boolean.parseBoolean(args.get("n"));
		}

		try {
			GitCorpusReader reader = null;
			if (args.containsKey("c")) {
				reader = new ShaFileGitCorpusReader(new File(args.get("c")), inputDir);
			} else if (args.containsKey("sl")) {
				reader = new StableLinuxGitCorpusReader(inputDir);
			} else {
				reader = new GitLogCorpusReader(inputDir, sinceStr, untilStr, fileStr);
			}

			SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, conf.tmpPath, new Configuration(), args.containsKey("ow"));
			DocumentFilter filter = new DocumentFilter(tokenizeAtUnderline, useNLTKStopwords);
			if (!args.containsKey("p") || !Boolean.parseBoolean(args.get("p"))) {
				StringBuilder sb = new StringBuilder("");
				while (reader.seekNext()) {
					sb.setLength(0);
					System.err.println("Writing commit " + reader.getSha());
					for (String para: filter.filterDocument(reader.getDoc())) {
						sb.append(para).append(' ');
					}
					writer.append(reader.getSha(), sb.toString());
				}
			} else {
				while (reader.seekNext()) {
					int pId = 0;
					System.err.println("Writing commit " + reader.getSha());
					for (String para: filter.filterDocument(reader.getDoc())) {
						writer.append(reader.getSha() + "-" + pId++, para);
					}
				}
			}
			writer.close();
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
