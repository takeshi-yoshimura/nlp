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
import ac.keio.sslab.analytics.PatchIDResolver;
import ac.keio.sslab.classification.GALowerClassifier;
import ac.keio.sslab.nlp.corpus.IdIndexReader;
import ac.keio.sslab.nlp.corpus.PatchEntryReader;
import ac.keio.sslab.utils.SimpleGitReader;

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
		Map<String, String> messages = new HashMap<>();
		SimpleGitReader g = new SimpleGitReader(gitDir);
		System.out.println("loading all the full messages in git");
		for (Entry<Integer, List<String>> e: new IdIndexReader(corpusDir).all().entrySet()) {
			for (String patchID: e.getValue()) {
				messages.put(patchID, g.getFullMessage(patchID));
			}
		}
		g.close();
		ClusterMetrics m = new ClusterMetrics(PatchIDResolver.getPointIDtoPatchIDs(corpusDir, bottomupDir), new PatchEntryReader(corpusDir).all(), messages);
		for (double ga = delta; ga < until; ga += delta) {
			performGA(ga, topDir, galower, dm, m);
		}
		performGA(2.0, topDir, galower, dm, m);
	}

	public void performGA(double ga, File topDir, GALowerClassifier galower, PatchDocMatcher dm, ClusterMetrics m) throws Exception {
		System.out.println("================== ga = " + ga + " =====================");
		File gaDir = new File(topDir, Double.toString(ga));
		PrintWriter pw = JobUtils.getPrintWriter(new File(topDir, "summary-ga-" + ga + ".txt"));
		gaDir.mkdirs();
		TreeMap<Integer, List<HierarchicalCluster>> lowers = galower.getAllGALowerClusters(ga);
		int i = 0;
		int total = 0;

		for (Entry<Integer, List<HierarchicalCluster>> lower: lowers.entrySet()) {
			for (@SuppressWarnings("unused") HierarchicalCluster c: lower.getValue()) {
				++total;
			}
		}
		for (Entry<Integer, List<HierarchicalCluster>> lower: lowers.entrySet()) {
			for (HierarchicalCluster c: lower.getValue()) {
				m.set(c);
				System.out.println("write Cluster ID = " + c.getID() + ", size = " + lower.getKey() + " (" + ++i + "/" + total + ")");
				m.writeJson(gaDir, dm);
				pw.println(m.toCSVString());
			}
		}
		pw.close();
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
