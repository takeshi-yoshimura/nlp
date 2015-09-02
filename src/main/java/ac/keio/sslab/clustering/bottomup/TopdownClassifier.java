package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.SimpleGitReader;

public class TopdownClassifier {

	List<HierarchicalCluster> clusters;
	SimpleGitReader git;
	Map<Integer, List<String>> realIDs;
	Map<String, String> versions;

	public TopdownClassifier(File clustersFile) throws IOException {
		clusters = ClusterGraph.parseResult(clustersFile).getClusters();
	}

	public void writeResultCSV(File outputDir, double ga) throws Exception {
		File summaryFile = new File(outputDir, "summary.csv");
		outputDir.mkdirs();
		PrintWriter p = JobUtils.getPrintWriter(summaryFile);
		TreeMap<Integer, List<HierarchicalCluster>> all = new TreeMap<>(Collections.reverseOrder());
		for (HierarchicalCluster c: clusters) {
			if (c.getParent() == null || c.getParent().getDensity() < ga) {
				continue;
			}
			if (!all.containsKey(c.size())) {
				all.put(c.size(), new ArrayList<HierarchicalCluster>());
			}
			all.get(c.size()).add(c);
			p.println(c.toString());
		}
		p.close();

		File allDir = new File(outputDir, "all");
		allDir.mkdirs();
		for (Entry<Integer, List<HierarchicalCluster>> e: all.entrySet()) {
			for (HierarchicalCluster c: e.getValue()) {
				File csv = new File(allDir, c.getID() + ".csv");
				PrintWriter pw = JobUtils.getPrintWriter(csv);
				writeClusterRecursive(pw, c);
				pw.close();
			}
		}
	}

	public void writeClusterRecursive(PrintWriter pw, HierarchicalCluster c) {
		if (c == null) {
			return;
		}
		pw.println(c.toString());
		writeClusterRecursive(pw, c.getLeft());
		writeClusterRecursive(pw, c.getRight());
	}
}
