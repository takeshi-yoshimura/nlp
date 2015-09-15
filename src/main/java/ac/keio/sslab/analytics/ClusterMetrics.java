package ac.keio.sslab.analytics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ac.keio.sslab.nlp.corpus.PatchEntryReader.PatchEntry;
import ac.keio.sslab.utils.SimpleGitReader;
import ac.keio.sslab.utils.SimpleJsonWriter;
import ac.keio.sslab.utils.SimpleSorter;

public class ClusterMetrics {

	HierarchicalCluster c;
	Map<String, Double> topicEx;
	List<HierarchicalCluster> singletons;
	Map<Integer, List<String>> resolver;
	Map<String, PatchEntry> patchEntries;

	public ClusterMetrics(Map<Integer, List<String>> resolver, Map<String, PatchEntry> patchEntries) {
		this.resolver = resolver;
		this.patchEntries = patchEntries;
	}

	public void set(HierarchicalCluster c) {
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

	public List<String> getPatchIDs() throws IOException {
		List<String> ret = new ArrayList<>();
		for (HierarchicalCluster singleton: singletons) {
			ret.addAll(resolver.get(singleton.getPoints().get(0)));
		}
		return ret;
	}

	public List<Entry<String, Integer>> getKeyFreqs(File gitDir, PatchDocMatcher m) throws IOException {
		Map<String, Integer> keyFreqs = new HashMap<>();
		for (String keyword: m.keySet()) {
			keyFreqs.put(keyword, 0);
		}
		SimpleGitReader g = new SimpleGitReader(gitDir);
		for (HierarchicalCluster singleton: singletons) {
			for (String patchID: resolver.get(singleton.getPoints().get(0))) {
				for (String s: m.match(g.getFullMessage(patchID))) {
					keyFreqs.put(s, keyFreqs.get(s) + 1);
				}
			}
		}
		return SimpleSorter.reverse(keyFreqs);
	}

	Map<String, Integer> files = null;
	Map<String, Integer> dates = null;
	Map<String, Integer> vers = null;

	public void getPatchEntries() throws IOException {
		for (String patchID: getPatchIDs()) {
			PatchEntry p = patchEntries.get(patchID);
			for (String file: p.files) {
				files.put(file, files.getOrDefault(file, 0) + 1);
			}
			dates.put(p.date, dates.getOrDefault(p.date, 0) + 1);
			vers.put(p.ver, vers.getOrDefault(p.ver, 0) + 1);
		}
	}

	public List<Entry<String, Integer>> getFiles() throws IOException {
		if (files == null) {
			getPatchEntries();
		}
		return SimpleSorter.reverse(files);
	}

	public List<Entry<String, Integer>> getDates() throws IOException {
		if (dates == null) {
			getPatchEntries();
		}
		return SimpleSorter.reverse(dates);
	}

	public List<Entry<String, Integer>> getVersions() throws IOException {
		if (vers == null) {
			getPatchEntries();
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

	public void writeJson(File dir, File gitDir, PatchDocMatcher m) throws IOException {
		SimpleJsonWriter w = new SimpleJsonWriter(new File(dir, Integer.toString(c.getID())));
		w.writeNumberField("size", size());
		w.writeNumberField("group average", ga());
		w.writeStartObject("topics");
		List<Entry<String, Integer>> et = getExpectedTopicNum();
		if (et != null) {
			for (Entry<String, Integer> e: getExpectedTopicNum()) {
				w.writeNumberField(e.getKey(), e.getValue());
			}
		}
		w.writeEndObject();
		w.writeStartObject("versions");
		for (Entry<String, Integer> e: getVersions()) {
			w.writeNumberField(e.getKey(), e.getValue());
		}
		w.writeEndObject();
		w.writeStartObject("files");
		for (Entry<String, Integer> e: getFiles()) {
			w.writeNumberField(e.getKey(), e.getValue());
		}
		w.writeEndObject();
		w.writeStartObject("keywords");
		for (Entry<String, Integer> e: getKeyFreqs(gitDir, m)) {
			if (e.getValue() > 0) {
				w.writeNumberField(e.getKey(), e.getValue());
			}
		}
		w.writeEndObject();
		w.close();
	}

	public String toCSVString() throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(c.getID()).append(',');
		sb.append(size()).append(',');
		sb.append(ga());
		return sb.toString();
	}
}
