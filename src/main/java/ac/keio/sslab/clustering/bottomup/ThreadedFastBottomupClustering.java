package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

// O(n) for calculation of minimum distance among all the clusters
// use around 8 * n^2 / 2 bytes RAM for caching (e.g., 400MB for 10K points)
public class ThreadedFastBottomupClustering extends FastBottomupClustering {

	final int numCore = Runtime.getRuntime().availableProcessors();
	Thread [] t = new Thread[numCore];

	public ThreadedFastBottomupClustering(List<Vector> idPoints, DistanceMeasure measure) throws Exception {
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
		for (int n_ = 0; n_ < numCore; n_++) {
			final int n = n_;
			t[n] = new Thread() {
				public void run() {
					for (int i = n; i < idPoints_.size(); i += numCore) {
						points.put(i, idPoints_.get(i));
						List<Integer> clusteredPoints = new ArrayList<Integer>();
						clusteredPoints.add(i);
						clusters.put(i, clusteredPoints);
					}
				}
			};
			t[n].start();
		}
		waitAll();
	}

	@Override
	public void initDistance(List<Vector> idPoints) throws InterruptedException {
		final List<Vector> idPoints_ = idPoints;
		final DistanceMeasure measure_ = measure;
		for (int n_ = 0; n_ < numCore; n_++) {
			final int n = n_;
			t[n] = new Thread() {
				public void run() {
					for (int i = n; i < clusters.size() - 1; i += numCore) {
						distance[i] = new double[i + 1];
						for (int j = 0; j <= i; j++) {
							distance[i][j] = measure_.distance(idPoints_.get(i + 1), idPoints_.get(j));
						}
					}
				}
			};
		}
		waitAll();
	}

	@Override
	public boolean next() {
		final double [] min_d_ = new double[numCore];
		final int [] min_i_ = new int[numCore];
		final int [] min_j_ = new int[numCore];

		for (int n_ = 0; n_ < numCore; n_++) {
			final int n = n_;
			t[n] = new Thread() {
				public void run() {
					min_d_[n] = Double.MAX_VALUE;
					min_i_[n] = -1; min_j_[n] = -1;
					for (int i: clusters.keySet()) {
						for (int j = n; j < i; j += numCore) {
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
