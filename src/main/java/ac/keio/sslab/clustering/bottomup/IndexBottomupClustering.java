package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.mahout.math.Vector;

public class IndexBottomupClustering extends Thread {

	TreeMap<Double, List<int []>> order;
	Map<Integer, List<Integer>> clusters; // Multimap cannot be used due to thread-safety
	List<Vector> points;
	int index;
	double d_min;

	Comparator<int []> sorter = new Comparator<int []>() {
		@Override
		public int compare(int[] o1, int[] o2) {
			if (o1[0] != o2[0]) return o1[0] - o2[0];
			return o1[1] - o2[1];
		}
	};

	public IndexBottomupClustering(List<Vector> points, int index) {
		this.points = points;
		order = new TreeMap<>();
		clusters = new HashMap<>();
		this.index = index;

		for (int i = 0; i < points.size(); i++)  {
			clusters.put(i, new ArrayList<Integer>());
			clusters.get(i).add(i);
			double d_min = Double.MAX_VALUE;
			int j_min = -1;
			for (int j = 0; j < i; j++) {
				double d = Math.abs(points.get(i).get(index) - points.get(j).get(index));
				if (d_min > d) {
					d_min = d;
					j_min = j;
				} else if (d_min == d && j_min > j) {
					j_min = j;
				}
			}
			if (j_min == -1) {
				continue;
			}
			if (order.containsKey(d_min)) {
				order.get(d_min).add(new int [] {i, j_min});
				Collections.sort(order.get(d_min), sorter);
			} else {
				order.put(d_min, new ArrayList<int[]>());
				order.get(d_min).add(new int [] {i, j_min});
			}
		}
	}

	public int[] popMostSimilarClusterPair() {
		if (order.isEmpty()) {
			return null;
		}
		List<int[]> tops = order.firstEntry().getValue();
		int [] top = tops.get(0);
		d_min = order.firstKey();
		if (tops.size() == 1) {
			order.remove(order.firstKey());
		} else {
			order.firstEntry().getValue().remove(0);
		}
		clusters.get(top[0]).addAll(clusters.get(top[1]));
		clusters.remove(top[1]);
		update(top[0], top[1]);
		return top;
	}

	public void update(int merging, int merged) {
		Map<int[], Double> updated = new HashMap<int[], Double>();
		List<Integer> recal = new ArrayList<Integer>();
		for (Iterator<Entry<Double, List<int[]>>> oIter = order.entrySet().iterator(); oIter.hasNext();) {
			Entry<Double, List<int[]>> e = oIter.next();
			for (Iterator<int[]> isIter = e.getValue().iterator(); isIter.hasNext();) {
				int [] i = isIter.next();
				if (i[0] == merged || i[0] == merging) {
					isIter.remove();
				} else if (i[1] == merging || i[1] == merged ) {
					isIter.remove();
					recal.add(i[0]);
				} else if (merging < i[0]) {
					double d = 0;
					for (int p1: clusters.get(i[0])) {
						for (int p2: clusters.get(merging)) {
							d += Math.abs(points.get(p1).get(index) - points.get(p2).get(index));
						}
					}
					d = d / clusters.get(i[0]).size() / clusters.get(merging).size();
					if (d < e.getKey()) {
						isIter.remove();
						updated.put(i, d);
					}
				}
			}
			if (e.getValue().isEmpty()) {
				oIter.remove();
			}
		}
		for (Entry<int[], Double> e: updated.entrySet()) {
			if (!order.containsKey(e.getValue())) {
				order.put(e.getValue(), new ArrayList<int[]>());
			}
			order.get(e.getValue()).add(e.getKey());
		}

		calcMin(merging);
		for (int rec: recal) {
			calcMin(rec);
		}
	}

	public void calcMin(int i) {
		double d_min = Double.MAX_VALUE;
		int j_min = -1;
		for (int j: clusters.keySet()) {
			if (i <= j) {
				continue;
			}
			double d = 0;
			for (int p1: clusters.get(i)) {
				for (int p2: clusters.get(j)) {
					d += Math.abs(points.get(p1).get(index) - points.get(p2).get(index));
				}
			}
			d = d / clusters.get(i).size() / clusters.get(j).size();
			if (d_min > d) {
				d_min = d;
				j_min = j;
			} else if (d_min == d && j_min > j) {
				j_min = j;
			}
		}
		if (j_min == -1) {
			return;
		}

		if (order.containsKey(d_min)) {
			order.get(d_min).add(new int [] {i, j_min});
			Collections.sort(order.get(d_min), sorter);
		} else {
			order.put(d_min, new ArrayList<int[]>());
			order.get(d_min).add(new int [] {i, j_min});
		}
	}

	public double getMaxSimilarity() {
		return d_min;
	}

	public Map<Integer, List<Integer>> getClusters() {
		return clusters;
	}
}
