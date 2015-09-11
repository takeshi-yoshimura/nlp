package ac.keio.sslab.nlp;

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

import com.google.common.collect.Lists;

import ac.keio.sslab.clustering.view.ClusterMetrics;
import ac.keio.sslab.clustering.view.GALowerClassifier;
import ac.keio.sslab.clustering.view.HierarchicalCluster;

public class ClusterMetricsJob extends SingletonGroupNLPJob {

	static Map<String, String> regex = new HashMap<>();
	static Map<String, List<String>> regexOr = new HashMap<>();
	static {
		// check perfect match to lower'ed words split by spaces and line wraps
		regex.put("Gen.Word.null", "null");
		regex.put("Gen.Word.lock", "lock");
		regex.put("Gen.Word.unlock", "unlock");
		regex.put("Gen.Word.deadlock", "deadlock");
		regex.put("Gen.Word.race", "race");
		regex.put("Gen.Ref.CVE", "cve-[0-9]+-[0-9]+");
		regex.put("Linux.Ref.httpbug", "http[s]*://bugzilla\\.kernel\\.org/show_bug\\.cgi?id=[0-9]+");
		regex.put("Linux.Oops.null", "bug: unable to handle kernel null pointer dereference at ");
		regex.put("Linux.Oops.pfault", "bug: unable to handle kernel paging request at ");
		regex.put("Linux.Oops.assert", "kernel bug at ");
		regex.put("Linux.Oops.gp", "general protection fault: ");
		regex.put("Linux.Warn.lockdep", "info: possible circular locking dependency detected");
		regex.put("Prog.func", "[a-z][0-9a-z_]*\\(\\)");
		regexOr.put("Prog.numtype", Lists.newArrayList("int", "long", "short", "char", "double", "float", "void"));
		regexOr.put("Prog.bufferoverflow", Lists.newArrayList("buffer overflow", "buffer overrun", "buffer underflow", "buffer underrun"));
		regexOr.put("Prog.array", Lists.newArrayList("array overrun", "array underrun", "index[- ]out[- ]of[- ]bound[s]*"));
		regexOr.put("Prog.corruption", Lists.newArrayList("memory corruption", "dangling pointer"));
		regexOr.put("Prog.memleak", Lists.newArrayList("memory leak", "memory leakage"));
		regexOr.put("Prog.concurrency", Lists.newArrayList("race", "racy", "race condition", "miss.*lock"));
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		System.out.println("Start at: " + new Date().toString());
		GALowerClassifier galower = new GALowerClassifier(new File(pMgr.getLocalOutputDir(), "clusters.csv"));
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
					ClusterMetrics m = ClusterMetrics.getExpectedNumOfTopics(c);
					if (m == null) {
						continue;
					}
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
		opts.addOption("k", "keywords", true, "keywords to extract");
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
