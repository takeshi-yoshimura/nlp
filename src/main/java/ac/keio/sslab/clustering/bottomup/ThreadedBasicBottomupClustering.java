package ac.keio.sslab.clustering.bottomup;

import java.util.List;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

//O(n^2) for calculation of minimum distance among all the clusters
// use around (8 * D) * n bytes RAM (e.g., 800MB for 1M 100D points)
public class ThreadedBasicBottomupClustering extends BasicBottomupClustering {

	final int numCore = Runtime.getRuntime().availableProcessors();
	Thread [] t = new Thread[numCore];

	public ThreadedBasicBottomupClustering(List<Vector> idPoints, DistanceMeasure measure) throws Exception {
		super(idPoints, measure);
	}

	public void waitAll() throws InterruptedException {
		for (int n = 0; n < numCore; n++) {
			t[n].join();
		}
	}

	@Override
	public void initCentroids(List<Vector> idPoints) throws InterruptedException {
		final List<Vector> idPoints_ = idPoints;
		for (int n_ = 0; n_ < numCore ; n_++) {
			final int n = n_;
			t[n] = new Thread() {
				public void run() {
					for (int i = n; i < idPoints_.size(); i+= numCore)  {
						centroids.put(i,  idPoints_.get(i));
					}
				}
			};
			t[n].start();
		}
		waitAll();
	}

	@Override
	public boolean next() {
		final int numCore = Runtime.getRuntime().availableProcessors();
		final double [] min_d_ = new double[numCore];
		final int min_i_[] = new int[numCore];
		final int min_j_[] = new int[numCore];
		Thread [] t = new Thread[numCore];

		for (int n_ = 0; n_ < numCore ; n_++) {
			final int n = n_;
			t[n] = new Thread() {
				public void run() {
					min_i_[n] = -1; min_j_[n] = -1;
					for (int i: centroids.keySet()) {
						for (int j = n; j < i; j += numCore) {
							if (!centroids.containsKey(j)) {
								continue;
							}
							double d = measure.distance(centroids.get(i), centroids.get(j));
							if (min_d_[n] > d) {
								min_d_[n] = d;
								min_i_[n] = i;
								min_j_[n] = j;
							}
						}
					}
				}
			};
			t[n].start();
		}

		double min_d = Double.MAX_VALUE;
		int min_i = -1, min_j = -1;
		try {
			for (int n = 0; n < numCore; n++) {
				t[n].join();
				if (min_d > min_d_[n]) {
					min_d = min_d_[n];
					min_i = min_i_[n];
					min_j = min_j_[n];
				}
			}
		} catch (InterruptedException e) {
			System.err.println("Interrupted");
			return false;
		}

		if (min_i == -1) {
			return false;
		}

		mergingPointId = min_j;
		mergedPointId = min_i;

		return true;
	}
}
