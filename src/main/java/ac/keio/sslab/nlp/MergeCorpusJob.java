package ac.keio.sslab.nlp;

import org.apache.commons.cli.Options;

public class MergeCorpusJob implements NLPJob {

	@Override
	public String getJobName() {
		return "mergeCorpus";
	}

	@Override
	public String getJobDescription() {
		return "merge separated corpora into one";
	}

	@Override
	public Options getOptions() {
		return null;
	}

	@Override
	public void run(JobManager mgr) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean runInBackground() {
		// TODO Auto-generated method stub
		return false;
	}

}
