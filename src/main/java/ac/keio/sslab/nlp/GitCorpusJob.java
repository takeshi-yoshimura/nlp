package ac.keio.sslab.nlp;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.corpus.GitLogCorpusReader;
import ac.keio.sslab.nlp.corpus.HashFileGitCorpusReader;
import ac.keio.sslab.nlp.corpus.PatchCorpusWriter;
import ac.keio.sslab.nlp.corpus.RepositoryReader;
import ac.keio.sslab.nlp.corpus.StableLinuxGitCorpusReader;

public class GitCorpusJob implements NLPJob {

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
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("g", "gitDir", true, "path to a git repository"));
		g.setRequired(true);

		Options options = new Options();
		options.addOptionGroup(g);
		options.addOption("s", "since", true, "Start object ref to be uploaded (yyyy/MM/dd). Default is blank (means all)");
		options.addOption("u", "until", true, "End object ref to be uploaded (yyyy/MM/dd). Default is HEAD.");
		options.addOption("f", "file", true, "target files or directory paths (comma-separated) in git repository. Default is the top of the input directory.");
		options.addOption("sl", "stableLinux", false, "Get all the commits for stable Linux (if specified, ignore -s and -u)");
		options.addOption("c", "commitFile", true, "File for commits to be extracted");
		options.addOption("t", "tokenizeAtUnderline", true, "tokenize at underline? (default is false)");
		options.addOption("n", "useNLTKStopwords", true, "use NLTK stopwords? (default is false)");
		options.addOption("p", "splitParagraph", true, "split paragraphs? (default is false)");
		return options;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		File outputDir = new File(conf.localCorpusFile, mgr.getJobID());
		File inputDir = new File(mgr.getArgStr("g"));
		Path outputPath = mgr.getJobIDPath(conf.corpusPath);
		String sinceStr = mgr.getArgOrDefault("s", "", String.class);
		String untilStr = mgr.getArgOrDefault("u", "HEAD", String.class);
		Set<String> fileStr = null;
		if (mgr.hasArg("f")) {
			fileStr = new HashSet<>();
			for (String f: mgr.getArgStr("f").split(",")) {
				fileStr.add(f);
			}
		}
		boolean tokenizeAtUnderline = mgr.getArgOrDefault("t", false, Boolean.class);
		boolean useNLTKStopwords = mgr.getArgOrDefault("n", false, Boolean.class);
		boolean splitParagraph = mgr.getArgOrDefault("p", false, Boolean.class);

		try {
			RepositoryReader reader = null;
			if (mgr.hasArg("c")) {
				reader = new HashFileGitCorpusReader(new File(mgr.getArgStr("c")), inputDir);
			} else if (mgr.hasArg("sl")) {
				reader = new StableLinuxGitCorpusReader(inputDir, fileStr);
			} else {
				reader = new GitLogCorpusReader(inputDir, sinceStr, untilStr, fileStr, false);
			}
			PatchCorpusWriter writer = new PatchCorpusWriter(outputDir, mgr.doForceWrite(), splitParagraph, tokenizeAtUnderline, useNLTKStopwords);

			while (reader.seekNext()) {
				System.out.println("write " + reader.getID());
				writer.processPatchMessage(reader.getID(), reader.getDate(), reader.getVersion(), reader.getFiles(), reader.getDoc());
			}
			writer.emitSummary(conf.hdfs, outputPath, conf.tmpPath, reader.getStats());
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
