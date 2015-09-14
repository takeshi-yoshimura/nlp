package ac.keio.sslab.nlp.job;

import org.apache.commons.cli.Options;

import ac.keio.sslab.nlp.corpus.PatchCorpusWriter;
import ac.keio.sslab.nlp.corpus.RepositoryReader;

public abstract class CorpusJob implements NLPJob {

	@Override
	public Options getOptions() {
		Options options = new Options();
		options.addOption("t", "tokenizeAtUnderline", true, "tokenize at underline? (default is false)");
		options.addOption("n", "useNLTKStopwords", true, "use NLTK stopwords? (default is false)");
		options.addOption("p", "splitParagraph", true, "split paragraphs? (default is false)");
		return options;
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		RepositoryReader reader = getReader(mgr, pMgr);
		PatchCorpusWriter writer = getWriter(mgr, pMgr);
		while (reader.seekNext()) {
			System.out.println("write ID: " + reader.getID());
			writer.process(reader);
		}
		writer.emitSummary(NLPConf.getInstance().hdfs, mgr.getHDFSOutputDir(), NLPConf.getInstance().tmpPath, reader.getStats());
		reader.close();
		writer.close();
	}

	protected PatchCorpusWriter getWriter(JobManager mgr, JobManager pMgr) throws Exception {
		boolean tokenizeAtUnderline = mgr.getArgOrDefault("t", false, Boolean.class);
		boolean useNLTKStopwords = mgr.getArgOrDefault("n", false, Boolean.class);
		boolean splitParagraph = mgr.getArgOrDefault("p", false, Boolean.class);
		return new PatchCorpusWriter(mgr.getLocalOutputDir(), mgr.doForceWrite(), splitParagraph, tokenizeAtUnderline, useNLTKStopwords);
	}

	protected abstract RepositoryReader getReader(JobManager mgr, JobManager pMgr) throws Exception;

	@Override
	public boolean runInBackground() {
		return false;
	}

	@Override
	public NLPJobGroup getJobGroup() {
		return new CorpusJobGroup();
	}
}
