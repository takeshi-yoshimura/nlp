package ac.keio.sslab.analytics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ac.keio.sslab.nlp.corpus.PatchEntryReader;
import ac.keio.sslab.utils.SimpleGitReader;
import ac.keio.sslab.utils.SimpleJsonWriter;
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

	public List<String> getPatchIDs(File corpusDir, File bottomupDir) throws IOException {
		List<String> ret = new ArrayList<>();
		Map<Integer, List<String>> resolver = PatchIDResolver.getPointIDtoPatchIDs(corpusDir, bottomupDir);
		for (HierarchicalCluster singleton: singletons) {
			ret.add(resolver.get(singleton.getPoints().get(0)).get(0));
		}
		return ret;
	}

	public List<Entry<String, Integer>> getKeyFreqs(File gitDir, File corpusDir, File bottomupDir, PatchDocMatcher m) throws IOException {
		Map<String, Integer> keyFreqs = new HashMap<>();
		for (String keyword: m.keySet()) {
			keyFreqs.put(keyword, 0);
		}
		Map<Integer, List<String>> resolver = PatchIDResolver.getPointIDtoPatchIDs(corpusDir, bottomupDir);
		SimpleGitReader g = new SimpleGitReader(gitDir);
		for (HierarchicalCluster singleton: singletons) {
			String patchID = resolver.get(singleton.getPoints().get(0)).get(0);
			for (String s: m.match(g.getFullMessage(patchID))) {
				keyFreqs.put(s, keyFreqs.get(s) + 1);
			}
		}
		return SimpleSorter.reverse(keyFreqs);
	}

	Map<String, Integer> files = null;
	Map<String, Integer> dates = null;
	Map<String, Integer> vers = null;

	public void getPatchEntries(File corpusDir, File bottomupDir) throws IOException {
		Set<String> patchIDs = new HashSet<>(getPatchIDs(corpusDir, bottomupDir));
		PatchEntryReader r = new PatchEntryReader(corpusDir);
		while(r.seekNext()) {
			if (patchIDs.contains(r.patchID())) {
				if (files == null) {
					files = new HashMap<>();
					dates = new HashMap<>();
					vers = new HashMap<>();
				}
				for (String file: r.files()) {
					files.put(file, files.getOrDefault(file, 0) + 1);
				}
				dates.put(r.date(), dates.getOrDefault(r.date(), 0) + 1);
				vers.put(r.version(), vers.getOrDefault(r.version(), 0) + 1);
			}
		}
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

	public void writeJson(File dir, File gitDir, File corpusDir, File bottomupDir, PatchDocMatcher m) throws IOException {
		SimpleJsonWriter w = new SimpleJsonWriter(new File(dir, Integer.toString(c.getID())));
		w.writeNumberField("size", size());
		w.writeNumberField("group average", ga());
		w.writeStartObject("topics");
		for (Entry<String, Integer> e: getExpectedTopicNum()) {
			w.writeNumberField(e.getKey(), e.getValue());
		}
		w.writeEndObject();
		w.writeStartObject("versions");
		for (Entry<String, Integer> e: getVersions(corpusDir, bottomupDir)) {
			w.writeNumberField(e.getKey(), e.getValue());
		}
		w.writeStartObject("files");
		for (Entry<String, Integer> e: getFiles(corpusDir, bottomupDir)) {
			w.writeNumberField(e.getKey(), e.getValue());
		}
		w.writeEndObject();
		w.writeStartObject("keywords");
		for (Entry<String, Integer> e: getKeyFreqs(gitDir, corpusDir, bottomupDir, m)) {
			w.writeNumberField(e.getKey(), e.getValue());
		}
		w.writeEndObject();
		w.close();
	}
}
