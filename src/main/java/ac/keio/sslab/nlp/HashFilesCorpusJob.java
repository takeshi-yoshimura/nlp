package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.nlp.corpus.HashFileGitCorpusReader;
import ac.keio.sslab.nlp.corpus.RepositoryReader;

public class HashFilesCorpusJob extends CorpusJob {

	@Override
	public String getAlgorithmName() {
		return "corpus.hash";
	}

	@Override
	public String getAlgorithmDescription() {
		return "Filter and upload corpus from the specified hash in git-log";
	}

	@Override
	public Options getOptions() {
		Options options = super.getOptions();
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("g", "gitDir", true, "path to a git repository"));
		g.setRequired(true);
		OptionGroup g2 = new OptionGroup();
		g2.addOption(new Option("c", "commitFile", true, "File for commits to be extracted"));
		g2.setRequired(true);

		options.addOptionGroup(g);
		options.addOptionGroup(g2);
		return options;
	}

	@Override
	protected RepositoryReader getReader(JobManager mgr) throws Exception {
		File inputDir = new File(mgr.getArgStr("g"));
		return new HashFileGitCorpusReader(new File(mgr.getArgStr("c")), inputDir);
	}
}
