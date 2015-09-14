package ac.keio.sslab.nlp.job;

import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.topdown.TopdownClustering;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;

public class TopDownJob extends ClusteringJobGroup implements NLPJob {

	@Override
	public String getAlgorithmName() {
		return "topdown";
	}

	@Override
	public String getAlgorithmDescription() {
		return "topdown kmeans";
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		LDAHDFSFiles hdfs = new LDAHDFSFiles(pMgr.getHDFSOutputDir());
		TopdownClustering topdown = new TopdownClustering();
		topdown.run(hdfs.documentPath, mgr.getHDFSOutputDir(), 32);
	}

	@Override
	public Options getOptions() {
		return null;
	}

	@Override
	public boolean runInBackground() {
		return true;
	}

	@Override
	public NLPJobGroup getJobGroup() {
		return new ClusteringJobGroup();
	}

}
