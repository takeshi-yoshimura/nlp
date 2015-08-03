package ac.keio.sslab.clustering.bottomup;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

// O(n) for calculation of minimum distance among all the clusters
// use around 8 * n^2 / 2 bytes RAM for caching (e.g., 400MB for 10K points)
public class InMemoryFastBottomupClustering implements BottomupClusteringListener {

	// {point id, point}
	double [][] distance;
	DistanceMeasure measure;

	// {owner point id, cluster centroid}
	Map<Integer, Vector> centroids;
	int mergingPointId, mergedPointId;
	Vector newVector;

	public InMemoryFastBottomupClustering(List<Vector> idPoints, DistanceMeasure measure) {
		this.measure = measure;
		centroids = new HashMap<Integer, Vector>();
		// initially all the clusters consist of one point (key: owener point index)
		for (int i = 0; i < idPoints.size(); i++) {
			centroids.put(i, idPoints.get(i));
		}

		distance = new double[centroids.size() - 1][];
		for (int i = 0; i < centroids.size() - 1; i++) {
			distance[i] = new double[i + 1];
			for (int j = 0; j <= i; j++) {
				distance[i][j] = measure.distance(idPoints.get(i + 1), idPoints.get(j));
			}
		}
	}

	@Override
	public boolean next() {
		double min_d = Double.MAX_VALUE;
		int min_i = -1, min_j = -1;
		for (int i: centroids.keySet()) {
			for (int j = 0; j < i; j++) {
				if (!centroids.containsKey(j)) {
					continue;
				}
				if (min_d > distance[i - 1][j]) {
					min_d = distance[i - 1][j];
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
		newVector = iV.plus(jV).divide(2);
		centroids.remove(min_i);
		centroids.put(min_j, newVector);

		for (int i = min_j; i < distance.length; i++) {
			if (centroids.containsKey(i + 1)) {
				distance[i][min_j] = measure.distance(centroids.get(i + 1), centroids.get(min_j));
			}
		}
		for (int j = 0; j < min_j; j++) {
			if (centroids.containsKey(j)) {
				distance[min_j - 1][j] = measure.distance(centroids.get(min_j), centroids.get(j));
			}
		}

		return true;
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

	@Override
	public Vector newPointVector() {
		return newVector;
	}
}
