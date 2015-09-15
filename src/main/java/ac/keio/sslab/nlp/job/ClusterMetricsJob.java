package ac.keio.sslab.nlp.job;

import java.io.File;
import java.io.PrintWriter;
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
		JobManager bottomupMgr = mgr.getParentJobManager();
		File bottomupDir = bottomupMgr.getLocalOutputDir();
		JobManager corpusMgr = bottomupMgr.getParentJobManager().getParentJobManager();
		File corpusDir = corpusMgr.getLocalOutputDir();
		File gitDir = new File(corpusMgr.getArgStr("g"));
		PatchDocMatcher dm = new PatchDocMatcher(new File(mgr.getArgStr("r")));

		double delta = mgr.getArgOrDefault("d", 0.05, Double.class);
		double until = mgr.getArgOrDefault("u", 0.5, Double.class);
		for (double ga = delta; ga < until; ga += delta) {
			System.out.println("================== ga = " + ga + " =====================");
			File gaDir = new File(topDir, Double.toString(ga));
			PrintWriter pw = JobUtils.getPrintWriter(new File(topDir, "summary-ga-" + ga + ".txt"));
			gaDir.mkdirs();
			TreeMap<Integer, List<HierarchicalCluster>> lowers = galower.getAllGALowerClusters(ga);
			for (Entry<Integer, List<HierarchicalCluster>> lower: lowers.entrySet()) {
				int i = 0;
				for (HierarchicalCluster c: lower.getValue()) {
					System.out.println("write Cluster ID = " + c.getID() + " (" + ++i + "/" + lowers.size() + ")");
					ClusterMetrics m = new ClusterMetrics(c);
					m.writeJson(gaDir, gitDir, corpusDir, bottomupDir, dm);
					pw.println(m.toCSVString(gitDir, corpusDir, bottomupDir, dm));
				}
			}
			pw.close();
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
