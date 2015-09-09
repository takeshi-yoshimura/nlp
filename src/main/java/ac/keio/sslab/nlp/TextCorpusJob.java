package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.nlp.corpus.RepositoryReader;
import ac.keio.sslab.nlp.corpus.SimpleTextCorpusReader;

public class TextCorpusJob extends CorpusJob {

	@Override
	public String getAlgorithmName() {
		return "corpus.text";
	}

	@Override
	public String getAlgorithmDescription() {
		return "create corpus from a text file (format is defined)";
	}

	@Override
	public Options getOptions() {
		Options options = super.getOptions();
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("i", "input", true, "Input file"));
		OptionGroup g2 = new OptionGroup();
		g2.addOption(new Option("s", "separator", true, "separator of ID and data sections"));
		g.setRequired(true);
		g2.setRequired(true);
		options.addOptionGroup(g);
		options.addOptionGroup(g2);
		return options;
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

	@Override
	protected RepositoryReader getReader(JobManager mgr, JobManager pMgr) throws Exception {
		File input = new File(mgr.getArgStr("i"));
		String s = mgr.getArgStr("s");
		return new SimpleTextCorpusReader(input, s);
	}

}
