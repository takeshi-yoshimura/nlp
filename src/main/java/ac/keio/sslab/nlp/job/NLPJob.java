package ac.keio.sslab.nlp.job;

import org.apache.commons.cli.Options;

public interface NLPJob {
	public String getAlgorithmName();
	public String getAlgorithmDescription();
	public void run(JobManager mgr, JobManager pMgr) throws Exception;
	public NLPJobGroup getJobGroup();
	public abstract Options getOptions();
	public abstract boolean runInBackground();
}
