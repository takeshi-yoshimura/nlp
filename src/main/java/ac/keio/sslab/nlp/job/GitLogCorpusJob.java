package ac.keio.sslab.nlp.job;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.nlp.corpus.GitLogCorpusReader;
import ac.keio.sslab.nlp.corpus.RepositoryReader;

public class GitLogCorpusJob extends CorpusJob {

	@Override
	public String getAlgorithmName() {
		return "corpus.gitlog";
	}

	@Override
	public String getAlgorithmDescription() {
		return "Filter and upload corpus from git log";
	}

	@Override
	public Options getOptions() {
		Options options = super.getOptions();
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("g", "gitDir", true, "path to a git repository"));
		g.setRequired(true);

		options.addOptionGroup(g);
		options.addOption("s", "since", true, "Start object ref to be uploaded (yyyy/MM/dd). Default is blank (means all)");
		options.addOption("u", "until", true, "End object ref to be uploaded (yyyy/MM/dd). Default is HEAD.");
		options.addOption("f", "file", true, "target files or directory paths (comma-separated) in git repository. Default is the top of the input directory.");
		return options;
	}

	@Override
	protected RepositoryReader getReader(JobManager mgr, JobManager pMgr) throws Exception {
		File inputDir = new File(mgr.getArgStr("g"));
		String sinceStr = mgr.getArgOrDefault("s", "", String.class);
		String untilStr = mgr.getArgOrDefault("u", "HEAD", String.class);
		Set<String> fileStr = null;
		if (mgr.hasArg("f")) {
			fileStr = new HashSet<>();
			for (String f: mgr.getArgStr("f").split(",")) {
				fileStr.add(f);
			}
		}
		return new GitLogCorpusReader(inputDir, sinceStr, untilStr, fileStr, false);
	}
}
