package ac.keio.sslab.clustering.bottomup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.SimpleGitReader;

public class BottomupClassifier {

	Map<Integer, HierarchicalCluster> singletons;
	Map<Integer, Map<String, Double>> pointTopics;
	SimpleGitReader git;
	Map<Integer, List<String>> realIDs;
	Map<String, String> versions;

	public BottomupClassifier(File clustersFile, File corpusIDIndexFile, File idIndexFile, File gitDir) throws IOException {
		List<HierarchicalCluster> singletons = ClusterGraph.parseResult(clustersFile).getSingletons();
		this.pointTopics = new HashMap<Integer, Map<String, Double>>();
		this.singletons = new HashMap<Integer, HierarchicalCluster>();
		for (HierarchicalCluster singleton: singletons) {
			this.singletons.put(singleton.getPoints().get(0), singleton);
			this.pointTopics.put(singleton.getPoints().get(0), singleton.getCentroid());
		}
		realIDs = getRealIDs(idIndexFile, corpusIDIndexFile);
		git = new SimpleGitReader(gitDir);
		versions = new HashMap<>();
		BufferedReader br = new BufferedReader(new FileReader(new File(idIndexFile.getParentFile(), "commits.txt")));
		String line = null;
		while ((line = br.readLine()) != null) {
			String [] s = line.split(",");
			versions.put(s[0], s[2]);
		}
		br.close();
	}

	public Set<Integer> getPointIDs() {
		return singletons.keySet();
	}

	public void writeAllDensityTrendCSV(File output) throws IOException {
		PrintWriter writer = JobUtils.getPrintWriter(output);
		StringBuilder sb = new StringBuilder();
		StringBuilder sb2 = new StringBuilder();
		for (Entry<Integer, HierarchicalCluster> c: singletons.entrySet()) {
			TreeMap<Integer, Double> trend = getDensityTrend(c.getKey());
			sb.setLength(0);
			sb2.setLength(0);
			sb.append("#Key=").append(c.getKey()).append('\n');
			for (Entry<Integer, Double> e: trend.entrySet()) {
				sb.append(e.getKey()).append(',');
				sb2.append(e.getValue()).append(',');
			}
			sb.setLength(sb.length() - 1);
			sb2.setLength(sb2.length() - 1);
			writer.println(sb.toString());
			writer.println(sb2.toString());
			writer.flush();
		}
		writer.close();
	}

	public TreeMap<Integer, Double> getDensityTrend(int pointID) {
		TreeMap<Integer, Double> trend = new TreeMap<Integer, Double>();
		HierarchicalCluster current = singletons.get(pointID);
		while (current != null) {
			trend.put(current.size(), current.getDensity());
			current = current.getParent();
		}
		return trend;
	}

	public void writeBestClusterJson(File outputDir, int pointID) throws Exception {
		String subject = git.getSubject(realIDs.get(pointID).get(0));
		List<String> shas = realIDs.get(pointID);
		Set<String> fileSet = new HashSet<String>();
		List<Date> dates = new ArrayList<Date>();
		List<String> versions = new ArrayList<String>();
		for (String sha: shas) {
			dates.add(git.getCommitDate(sha));
			versions.add(this.versions.get(sha));
			fileSet.addAll(git.getFiles(sha));
		}
		List<String> files = new ArrayList<String>(fileSet);
		Map<String, Double> topic = singletons.get(pointID).getCentroid();
		List<HierarchicalCluster> all = getBestCluster(pointID);
		IncrementalClusterDumper c = new IncrementalClusterDumper(all, pointTopics, realIDs, git);
		new PointDumper(pointID, subject, dates, versions, shas, files, topic, c).writeJson(outputDir);
	}

	public Map<Integer, List<String>> getRealIDs(File idIndexFile, File corpusIDIndexFile) throws IOException {
		Map<String, Integer> corpusIDMap = new HashMap<String, Integer>();
		BufferedReader br = new BufferedReader(new FileReader(corpusIDIndexFile));
		String line = null;
		while((line = br.readLine()) != null) {
			String [] s = line.split(",");
			corpusIDMap.put(s[1], Integer.parseInt(s[0]));
		}
		br.close();

		Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
		BufferedReader reader = new BufferedReader(new FileReader(idIndexFile));
		while ((line = reader.readLine()) != null) {
			String [] s = line.split("\t\t");
			String [] s2 = s[1].split(",");
			List<String> a = new ArrayList<String>();
			for (int i = 0; i < s2.length; i++) {
				if (s2[i].isEmpty()) {
					continue;
				}
				a.add(s2[i]);
			}
			ret.put(corpusIDMap.get(s[0]), a);
		}
		reader.close();

		return ret;
	}

	public List<HierarchicalCluster> getBestCluster(int pointID) {
		TreeMap<Integer, HierarchicalCluster> scores = new TreeMap<Integer, HierarchicalCluster>();
		HierarchicalCluster current = singletons.get(pointID);
		HierarchicalCluster parent = current.getParent();

		while (current != null && parent != null) {
			scores.put(parent.size() - current.size(), current);
			current = parent;
			parent = parent.getParent();
		}
		HierarchicalCluster best = scores.lastEntry().getValue();

		List<HierarchicalCluster> all = new ArrayList<HierarchicalCluster>();
		current = singletons.get(pointID);
		parent = current.getParent();
		while (current != null && parent != null && current != best) {
			all.add(current);
			current = parent;
			parent = parent.getParent();
		}
		all.add(best);

		return all;
	}
}
