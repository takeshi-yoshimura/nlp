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
		for (int i = 0; i < idPoints.size() - 1; i++) {
			distance[i] = new double[i + 1];
			for (int j = 0; j <= i; j++) {
				distance[i][j] = measure.distance(idPoints.get(i + 1), idPoints.get(j));
			}
		}
	}

	public FastBottomupClustering(List<Vector> idPoints, DistanceMeasure measure) throws Exception {
		super(idPoints, measure);
		distance = new double[idPoints.size() - 1][];
		initDistance(idPoints);
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
						d += distance[p][p2];
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
}
