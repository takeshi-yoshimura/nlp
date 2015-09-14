package ac.keio.sslab.nlp.job;

import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.analytics.ClusterMetrics;
import ac.keio.sslab.analytics.HierarchicalCluster;
import ac.keio.sslab.analytics.PatchDocMatcher;
import ac.keio.sslab.classification.GALowerClassifier;

public class ClusterMetricsJob extends SingletonGroupNLPJob {

	Map<String, String> regex = new HashMap<>();

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		System.out.println("Start at: " + new Date().toString());
		GALowerClassifier galower = new GALowerClassifier(pMgr.getLocalOutputDir());
		File topDir = mgr.getLocalOutputDir();
		File bottomupDir = mgr.getParentJobManager().getLocalOutputDir();
		File corpusDir = mgr.getParentJobManager().getParentJobManager().getLocalOutputDir();
		PatchDocMatcher dm = new PatchDocMatcher(new File(mgr.getArgStr("r")));

		double delta = mgr.getArgOrDefault("d", 0.05, Double.class);
		double until = mgr.getArgOrDefault("u", 0.5, Double.class);
		for (double ga = delta; ga < until; ga += delta) {
			File gaDir = new File(topDir, Double.toString(ga));
			TreeMap<Integer, List<HierarchicalCluster>> lowers = galower.getAllGALowerClusters(ga);
			for (Entry<Integer, List<HierarchicalCluster>> lower: lowers.entrySet()) {
				for (HierarchicalCluster c: lower.getValue()) {
					ClusterMetrics m = new ClusterMetrics(c);
					m.writeJson(gaDir, corpusDir, bottomupDir, dm);
				}
			}
		}
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.setRequired(true);
		g.addOption(new Option("r", "regexFile", true, "docregex.txt"));
		Options opts = new Options();
		opts.addOption("d", "delta", true, "delta group average to explore better clusters (default = 0.05)");
		opts.addOption("u", "until", true, "explore until this group average (default = 0.5)");
		opts.addOptionGroup(g);
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
