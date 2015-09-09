package ac.keio.sslab.nlp;

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
	public void run(JobManager mgr) throws Exception {
		JobManager topdownMgr = mgr.getParentJobManager();
		LDAHDFSFiles hdfs = new LDAHDFSFiles(topdownMgr.getHDFSOutputDir());
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
