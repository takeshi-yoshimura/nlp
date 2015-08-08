package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

//O(n^2) for calculation of minimum distance among all the clusters
// use around (8 * D) * n bytes RAM (e.g., 800MB for 1M 100D points)
public class BasicBottomupClustering implements BottomupClusteringAlgorithm {

	// {point id, point}
	DistanceMeasure measure;

	// {owner point id, cluster centroid}
	Map<Integer, List<Integer>> clusters;
	Map<Integer, Vector> points;
	int mergingPointId, mergedPointId;

	// can override
	public void initCentroids(List<Vector> idPoints) throws Exception {
		// initially all the clusters consist of one point (key: owener point id)
		for (int i = 0; i < idPoints.size(); i++) {
			points.put(i, idPoints.get(i));
			List<Integer> clusteredPoints = new ArrayList<Integer>();
			clusteredPoints.add(i);
			clusters.put(i, clusteredPoints);
		}
	}

	public BasicBottomupClustering(List<Vector> idPoints, DistanceMeasure measure) throws Exception {
		this.measure = measure;
		clusters = new HashMap<Integer, List<Integer>>();
		points = new HashMap<Integer, Vector>();
		initCentroids(idPoints);
	}

	@Override
	public boolean next() {
		double min_d = Double.MAX_VALUE;
		int min_i = -1, min_j = -1;
		for (int i: clusters.keySet()) {
			for (int j = 0; j < i; j++) {
				if (!clusters.containsKey(j)) {
					continue;
				}
				double d = 0;
				for (int p: clusters.get(i)) {
					for (int p2: clusters.get(j)) {
						d += measure.distance(points.get(p), points.get(p2));
					}
				}
				d = d / clusters.get(i).size() / clusters.get(j).size();
				if (min_d > d) {
					min_d = d;
					min_i = i;
					min_j = j;
				}
			}
		}

		if (min_i == -1) {
			return false;
		}

		mergingPointId = min_j;
		mergedPointId = min_i;

		return true;
	}

	@Override
	public Vector update() {
		List<Integer> newCluster = clusters.get(mergingPointId);
		newCluster.addAll(clusters.get(mergedPointId));
		clusters.remove(mergedPointId);
		Vector centroid = points.get(newCluster.get(0));
		for (int i = 1; i < newCluster.size(); i++) {
			centroid = centroid.plus(points.get(newCluster.get(i)));
		}
		return centroid.divide(newCluster.size());
	}

	@Override
	public int mergingPointId() {
		return mergingPointId;
	}

	@Override
	public int mergedPointId() {
		return mergedPointId;
	}

	@Override
	public DistanceMeasure getDistanceMeasure() {
		return measure;
	}
}
