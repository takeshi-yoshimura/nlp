package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.mahout.math.Vector;

public class NaiveBottomupClustering {

	TreeMap<Integer, List<Integer>> clusters;
	List<Vector> points;
	int mergingPointId, mergedPointId;

	public NaiveBottomupClustering(List<Vector> points) {
		this.points = points;
		clusters = new TreeMap<Integer, List<Integer>>();
		for (int i = 0; i < points.size(); i++) {
			clusters.put(i, new ArrayList<Integer>());
			clusters.get(i).add(i);
		}
	}

	public int [] next() {
		double min_d = Double.MAX_VALUE;
		int min_i = -1, min_j = -1;
		for (int i: clusters.keySet()) {
			for (int j = 0; j < i; j++) {
				if (!clusters.containsKey(j)) {
					continue;
				}
				double d = getSimilarity(i, j);
				if (min_d > d) {
					min_d = d;
					min_i = i;
					min_j = j;
				} else if (min_d == d) {
					if(min_i > i || (min_i == i && min_j > j)) {
						min_d = d;
						min_i = i;
						min_j = j;
					}
				}
			}
		}

		if (min_i == -1) {
			return null;
		}
		currentMinS = min_d;

		clusters.get(min_i).addAll(clusters.remove(min_j));

		return new int [] {min_i, min_j};
	}

	// use Group Average as cluster similarity measure
	protected double getSimilarity(int cluster1, int cluster2) {
		double d = 0;
		for (int p1: clusters.get(cluster1)) {
			for (int p2: clusters.get(cluster2)) {
				d += points.get(p1).dot(points.get(p2));
			}
		}
		int N1 = clusters.get(cluster1).size();
		int N2 = clusters.get(cluster2).size();
		return d / (N1 + N2) / (N1 + N2 - 1);
	}

	public Map<Integer, List<Integer>> getClusters() {
		return clusters;
	}

	double currentMinS;
}