package ac.keio.sslab.clustering.bottomup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

//O(n^2) for calculation of minimum distance among all the clusters
// use around (8 * D) * n bytes RAM (e.g., 800MB for 1M 100D points)
public class InMemorySlowBottomupClustering {

	// {point id, point}
	DistanceMeasure measure;

	// {owner point id, cluster centroid}
	Map<Integer, Vector> centroids;
	int mergingPointId, mergedPointId;

	public InMemorySlowBottomupClustering(List<Vector> idPoints, DistanceMeasure measure) {
		this.measure = measure;
		centroids = new HashMap<Integer, Vector>(); 
		// initially all the clusters consist of one point (key: owener point id)
		for (int i = 0; i < idPoints.size(); i++) {
			centroids.put(i, idPoints.get(i));
		}
	}

	public boolean next() {
		double min_d = Double.MAX_VALUE;
		int min_i = -1, min_j = -1;
		for (int i: centroids.keySet()) {
			for (int j = 0; j < i; j++) {
				if (!centroids.containsKey(j)) {
					continue;
				}
				double d = measure.distance(centroids.get(i), centroids.get(j));
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

		Vector iV = centroids.get(min_i);
		Vector jV = centroids.get(min_j);
		Vector newV = iV.plus(jV).divide(2);
		centroids.remove(min_i);
		centroids.put(min_j, newV);

		return true;
	}

	public int mergingPointId() {
		return mergingPointId;
	}

	public int mergedPointId() {
		return mergedPointId;
	}
}
