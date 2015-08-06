package ac.keio.sslab.clustering.bottomup;

import java.util.List;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

// O(n) for calculation of minimum distance among all the clusters
// use around 8 * n^2 / 2 bytes RAM for caching (e.g., 400MB for 10K points)
public class FastBottomupClustering extends BasicBottomupClustering {

	// {point id, point}
	double [][] distance;

	// can override algorithm
	public void initDistance(List<Vector> idPoints) throws Exception {
		for (int i = 0; i < centroids.size() - 1; i++) {
			distance[i] = new double[i + 1];
			for (int j = 0; j <= i; j++) {
				distance[i][j] = measure.distance(idPoints.get(i + 1), idPoints.get(j));
			}
		}
	}

	public FastBottomupClustering(List<Vector> idPoints, DistanceMeasure measure) throws Exception {
		super(idPoints, measure);
		distance = new double[centroids.size() - 1][];
		initDistance(idPoints);
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

		return true;
	}

	// can override algorithm
	public void updateDistance() throws Exception {
		for (int i = mergingPointId; i < distance.length; i++) {
			if (centroids.containsKey(i + 1)) {
				distance[i][mergingPointId] = measure.distance(centroids.get(i + 1), centroids.get(mergingPointId));
			}
		}
		for (int j = 0; j < mergingPointId; j++) {
			if (centroids.containsKey(j)) {
				distance[mergingPointId - 1][j] = measure.distance(centroids.get(mergingPointId), centroids.get(j));
			}
		}
	}

	@Override
	public Vector update() {
		Vector newVector = super.update();

		try {
			updateDistance();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		return newVector;
	}
}
