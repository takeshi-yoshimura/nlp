package ac.keio.sslab.nlp.job;

import java.io.File;
import java.io.PrintWriter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.analytics.ClusterMetrics;
import ac.keio.sslab.analytics.HierarchicalCluster;
import ac.keio.sslab.classification.GALowerClassifier;

public class ClusterMetricsJob extends SingletonGroupNLPJob {

	Map<String, String> regex = new HashMap<>();

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		System.out.println("Start at: " + new Date().toString());
		GALowerClassifier galower = new GALowerClassifier(pMgr.getLocalOutputDir());
		PrintWriter pw = JobUtils.getPrintWriter(mgr.getLocalOutputDir());
		Map<Integer, ClusterMetrics> metrics = new HashMap<>();

		double delta = mgr.getArgOrDefault("d", 0.05, Double.class);
		double until = mgr.getArgOrDefault("u", 0.5, Double.class);
		for (double ga = delta; ga < until; ga += delta) {
			pw.println("----------------- cut off by ga = " + ga + " -------------------------");
			TreeMap<Integer, List<HierarchicalCluster>> lowers = galower.getAllGALowerClusters(ga);
			for (Entry<Integer, List<HierarchicalCluster>> lower: lowers.entrySet()) {
				for (HierarchicalCluster c: lower.getValue()) {
					if (metrics.containsKey(c.getID())) {
						continue;
					}
					ClusterMetrics m = new ClusterMetrics(c);
					metrics.put(c.getID(), m);
				}
			}
		}
		pw.close();
	}

	@Override
	public Options getOptions() {
		Options opts = new Options();
		opts.addOption("d", "delta", true, "delta group average to explore better clusters (default = 0.05)");
		opts.addOption("u", "until", true, "explore until this group average (default = 0.5)");
		opts.addOption("r", "regexFile", true, "docregex.txt");
		return opts;
	}

	@Override
	public boolean runInBackground() {
		return true;
	}

	@Override
	public String getJobName() {
		return "bottomup.cmetrics";
	}

	@Override
	public String getShortJobName() {
		return "cm";
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
		return "extract cluster metrics";
	}
}