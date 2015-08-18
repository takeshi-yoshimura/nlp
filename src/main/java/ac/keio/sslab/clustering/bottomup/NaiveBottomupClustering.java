package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

public class NaiveBottomupClustering {

	DistanceMeasure measure;
	TreeMap<Integer, List<Integer>> clusters;
	List<Vector> points;
	int mergingPointId, mergedPointId;

	public NaiveBottomupClustering(List<Vector> points, DistanceMeasure measure) {
		this.points = points;
		this.measure = measure;
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
				double d = 0;
				for (int p1: clusters.get(i)) {
					for (int p2: clusters.get(j)) {
						d += measure.distance(points.get(p1), points.get(p2));
					}
				}
				d = d / clusters.get(i).size() / clusters.get(j).size();
				if (min_d > d) {
					min_d = d;
					min_i = i;
					min_j = j;
				} else if (min_d == d && min_i < i) {
					min_d = d;
					min_i = i;
					min_j = j;
				}
			}
		}

		if (min_i == -1) {
			return null;
		}

		clusters.get(min_i).addAll(clusters.remove(min_j));

		return new int [] {min_i, min_j};
	}

	public Map<Integer, List<Integer>> getClusters() {
		return clusters;
	}
}