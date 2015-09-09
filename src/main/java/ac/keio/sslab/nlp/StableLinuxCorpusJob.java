package ac.keio.sslab.nlp;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.nlp.corpus.RepositoryReader;
import ac.keio.sslab.nlp.corpus.StableLinuxGitCorpusReader;

public class StableLinuxCorpusJob extends GitLogCorpusJob {

	@Override
	public String getAlgorithmName() {
		return "corpus.linux.stable";
	}

	@Override
	public Options getOptions() {
		Options options = super.getOptions();
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("g", "gitDir", true, "path to a git repository"));
		g.setRequired(true);

		options.addOptionGroup(g);
		options.addOption("f", "file", true, "target files or directory paths (comma-separated) in git repository. Default is the top of the input directory.");
		return options;
	}

	@Override
	public String getAlgorithmDescription() {
		return "Filter and upload corpus from only stable patches for Linux in git-log";
	}

	@Override
	protected RepositoryReader getReader(JobManager mgr) throws Exception {
		File inputDir = new File(mgr.getArgStr("g"));
		Set<String> fileStr = null;
		if (mgr.hasArg("f")) {
			fileStr = new HashSet<>();
			for (String f: mgr.getArgStr("f").split(",")) {
				fileStr.add(f);
			}
		}
		return new StableLinuxGitCorpusReader(inputDir, fileStr);
	}
}
