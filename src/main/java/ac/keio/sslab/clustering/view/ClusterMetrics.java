package ac.keio.sslab.clustering.view;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ac.keio.sslab.nlp.corpus.PatchCorpusReader;
import ac.keio.sslab.nlp.corpus.PatchCorpusReader.PatchEntry;
import ac.keio.sslab.utils.SimpleGitReader;
import ac.keio.sslab.utils.SimpleSorter;

public class ClusterMetrics {

	HierarchicalCluster c;
	Map<String, Double> topicEx;
	List<HierarchicalCluster> singletons;

	public ClusterMetrics(HierarchicalCluster c) {
		this.c = c;
		this.singletons = searchSingleton(c);
		this.topicEx = getTopicEx(c);
	}

	protected List<HierarchicalCluster> searchSingleton(HierarchicalCluster c) {
		List<HierarchicalCluster> l = new ArrayList<>();
		traverseCluster(l, c);
		return l;
	}

	protected void traverseCluster(List<HierarchicalCluster> result, HierarchicalCluster c) {
		if (c.size() == 1) {
			result.add(c);
			return;
		}
		traverseCluster(result, c.getLeft());
		traverseCluster(result, c.getRight());
	}

	protected Map<String, Double> getTopicEx(HierarchicalCluster c) {
		Map<String, Double> real = new HashMap<>();
		for (HierarchicalCluster singleton: singletons) {
			for (Entry<String, Double> e: singleton.getCentroid().entrySet()) {
				if (!real.containsKey(e.getKey())) {
					real.put(e.getKey(), e.getValue());
				} else {
					real.put(e.getKey(), real.get(e.getKey()) + e.getValue());
				}
			}
		}
		return real;
	}

	public List<Integer> getPointIDs() {
		List<Integer> ret = new ArrayList<>();
		for (HierarchicalCluster singleton: singletons) {
			ret.add(singleton.getPoints().get(0));
		}
		return ret;
	}

	public List<String> getPatchIDs(File idIndexFile, File corpusIDIndexFile) throws IOException {
		List<String> ret = new ArrayList<>();
		Map<Integer, List<String>> resolver = new PatchIDResolver(idIndexFile, corpusIDIndexFile).getPointIDtoPatchIDs();
		for (HierarchicalCluster singleton: singletons) {
			ret.add(resolver.get(singleton.getPoints().get(0)).get(0));
		}
		return ret;
	}

	public List<Entry<String, Integer>> getKeyFreqs(File gitDir, File idIndexFile, File corpusIDIndexFile, Collection<String> keywords) throws IOException {
		Map<String, Integer> keyFreqs = new HashMap<>();
		for (String keyword: keywords) {
			keyFreqs.put(keyword, 0);
		}
		SimpleGitReader g = new SimpleGitReader(gitDir);
		Map<Integer, List<String>> resolver = new PatchIDResolver(idIndexFile, corpusIDIndexFile).getPointIDtoPatchIDs();
		for (HierarchicalCluster singleton: singletons) {
			String hash = resolver.get(singleton.getPoints().get(0)).get(0);
			for (String keyword: keywords) {
				if (g.msgMatches(hash, keyword)) {
					keyFreqs.put(keyword, keyFreqs.get(keyword) + 1);
				}
			}
		}
		g.close();
		return SimpleSorter.reverse(keyFreqs);
	}

	Map<String, Integer> files = null;
	Map<String, Integer> dates = null;
	Map<String, Integer> vers = null;

	public Map<String, PatchEntry> getPatchEntries(File corpusDir, File bottomupDir) throws IOException {
		Map<String, PatchEntry> ret = new HashMap<>();
		Set<String> patchIDs = new HashSet<>(getPatchIDs(new File(corpusDir, "idIndex.txt"), new File(bottomupDir, "corpusIDIndex.txt")));
		for (Entry<String, PatchEntry> e: new PatchCorpusReader(corpusDir).getPatchEntries().entrySet()) {
			if (patchIDs.contains(e.getKey())) {
				if (files == null) {
					files = new HashMap<>();
					dates = new HashMap<>();
					vers = new HashMap<>();
				}
				ret.put(e.getKey(), e.getValue());
				for (String file: e.getValue().files) {
					files.put(file, files.getOrDefault(file, 0) + 1);
				}
				dates.put(e.getValue().date, dates.getOrDefault(e.getValue().date, 0) + 1);
				vers.put(e.getValue().ver, vers.getOrDefault(e.getValue().ver, 0) + 1);
			}
		}
		return ret;
	}

	public List<Entry<String, Integer>> getFiles(File corpusDir, File bottomupDir) throws IOException {
		if (files == null) {
			getPatchEntries(corpusDir, bottomupDir);
		}
		return SimpleSorter.reverse(files);
	}

	public List<Entry<String, Integer>> getDates(File corpusDir, File bottomupDir) throws IOException {
		if (dates == null) {
			getPatchEntries(corpusDir, bottomupDir);
		}
		return SimpleSorter.reverse(dates);
	}

	public List<Entry<String, Integer>> getVersions(File corpusDir, File bottomupDir) throws IOException {
		if (vers == null) {
			getPatchEntries(corpusDir, bottomupDir);
		}
		return SimpleSorter.reverse(vers);
	}

	public List<Entry<String, Integer>> getExpectedTopicNum() {
		Map<String, Integer> count = new HashMap<>();
		for (Entry<String, Double> e: topicEx.entrySet()) {
			int i = e.getValue().intValue();
			if (i < 1) {
				continue;
			}
			count.put(e.getKey(), i);
		}
		if (count.isEmpty()) {
			return null;
		}

		return SimpleSorter.reverse(count);
	}

	public List<HierarchicalCluster> getSingletonsOrderedByDistanceToCentroid() {
		Map<HierarchicalCluster, Double> map = new HashMap<>();
		for (HierarchicalCluster singleton: singletons) {
			double distance = 0.0;
			for (Entry<String, Double> e: topicEx.entrySet()) {
				double d;
				if (!singleton.getCentroid().containsKey(e.getKey())) {
					d = e.getValue();
				} else {
					d = e.getValue() - singleton.getCentroid().get(e.getKey());
				}
				distance += d * d;
			}
			map.put(singleton, distance);
		}
		List<HierarchicalCluster> ret = new ArrayList<>();
		for (Entry<HierarchicalCluster, Double> m: SimpleSorter.reverse(map)) {
			ret.add(m.getKey());
		}
		return ret;
	}

	public int size() {
		return c.size();
	}

	public double ga() {
		return c.getDensity();
	}
}
