package ac.keio.sslab.clustering.bottomup;

import java.util.List;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

public class DistanceCache {

	// {point id, point}
	double [][] distance;
	DistanceMeasure measure;
	int cachedRowEnd;
	List<Vector> points;

	public DistanceCache(List<Vector> points, DistanceMeasure measure, long memoryCapacity) throws InterruptedException {
		this.measure = measure;
		initDistance(points, memoryCapacity);
	}

	public void waitAll(Thread [] t) throws InterruptedException {
		for (int n = 0; n < t.length; n++) {
			t[n].join();
		}
	}

	public void initDistance(List<Vector> points, long memoryCapacity) throws InterruptedException {
		final List<Vector> points_ = points;
		final DistanceMeasure measure_ = measure;
		final int numCore = Runtime.getRuntime().availableProcessors();

		long memoryUse = 0;
		cachedRowEnd = 0;
		while (memoryUse < memoryCapacity) {
			memoryUse += Double.SIZE / 8 * ++cachedRowEnd;
		}

		Thread [] t = new Thread[numCore];
		for (int n_ = 0; n_ < numCore; n_++) {
			final int n = n_;
			final long last = cachedRowEnd;
			t[n] = new Thread() {
				public void run() {
					for (int i = n; i < points_.size() - 1 && i < last; i += numCore) {
						distance[i] = new double[i + 1];
						for (int j = 0; j <= i; j++) {
							distance[i][j] = measure_.distance(points_.get(i + 1), points_.get(j));
						}
					}
				}
			};
		}
		waitAll(t);
	}

	public double get(int i, int j) {
		if (i < cachedRowEnd) {
			return distance[i][j];
		}
		return measure.distance(points.get(i), points.get(j));
	}
}
