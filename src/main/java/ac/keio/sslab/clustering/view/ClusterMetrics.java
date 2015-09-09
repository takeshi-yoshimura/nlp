package ac.keio.sslab.clustering.view;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ClusterMetrics {

	List<Entry<String, Integer>> expectedTopicNums;
	int size;
	double ga;
	int centroidNeighbor;

	public ClusterMetrics(int size, double ga, int neighbor, List<Entry<String, Integer>> expectedNumOfTopics) {
		this.size = size;
		this.ga = ga;
		this.centroidNeighbor = neighbor;
		this.expectedTopicNums = expectedNumOfTopics;
	}

	static protected void searchSingleton(List<HierarchicalCluster> result, HierarchicalCluster c) {
		if (c.size() == 1) {
			result.add(c);
			return;
		}
		searchSingleton(result, c.getLeft());
		searchSingleton(result, c.getRight());
	}

	static public ClusterMetrics getExpectedNumOfTopics(HierarchicalCluster c) {
		Comparator<Entry<String, Integer>> reverser = new Comparator<Entry<String, Integer>>() {
		    @Override
		    public int compare(Entry<String,Integer> entry1, Entry<String,Integer> entry2) {
		        return entry2.getValue().compareTo(entry1.getValue());
		    }
		};
		List<HierarchicalCluster> singletons = new ArrayList<>();
		searchSingleton(singletons, c);
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

		Map<String, Integer> count = new HashMap<>();
		for (Entry<String, Double> e: real.entrySet()) {
			int i = e.getValue().intValue();
			if (i < 1) {
				continue;
			}
			count.put(e.getKey(), i);
		}
		if (count.isEmpty()) {
			return null;
		}

		List<Map.Entry<String, Integer>> exp = new ArrayList<>(count.entrySet());
		Collections.sort(exp, reverser);

		double min_d = Double.MIN_VALUE;
		int neighbor = -1;
		for (HierarchicalCluster singleton: singletons) {
			double distance = 0.0;
			for (Entry<String, Double> e: real.entrySet()) {
				double d;
				if (!singleton.getCentroid().containsKey(e.getKey())) {
					d = e.getValue();
				} else {
					d = e.getValue() - singleton.getCentroid().get(e.getKey());
				}
				distance += d * d;
			}
			if (min_d > distance) {
				min_d = distance;
				neighbor = singleton.getID();
			}
		}
		return new ClusterMetrics(c.size(), c.getDensity(), neighbor, exp);
	}
}
