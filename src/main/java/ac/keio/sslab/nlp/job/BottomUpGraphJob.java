package ac.keio.sslab.nlp.job;

import java.io.File;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.analytics.ClusterGraphDumper;

public class BottomUpGraphJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "bottomup.graph";
	}

	@Override
	public String getShortJobName() {
		return "bgr";
	}

	@Override
	public NLPJobGroup getParentJobGroup() {
		return new BottomUpJob();
	}

	@Override
	public File getLocalJobDir() {
		return new File(NLPConf.getInstance().finalOutputFile, getJobName());
	}

	@Override
	public Path getHDFSJobDir() {
		return null;
	}

	@Override
	public String getJobDescription() {
		return "bottomup clustering graph viewer";
	}

	@Override
	public Options getOptions() {
		Options opt = new Options();
		opt.addOption("s", "startID", true, "starting cluster ID for graph (default: root)");
		opt.addOption("n", "numHierarchy", true, "number of Hierarchy to view (default: 5)");
		return opt;
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		ClusterGraphDumper v= new ClusterGraphDumper(pMgr.getLocalOutputDir());
		v.dumpPDF(mgr.getLocalOutputDir(), mgr.getArgOrDefault("s", v.getRootID(), Integer.class), mgr.getArgOrDefault("n", 5, Integer.class), true);
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
