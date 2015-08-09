package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

public class CachedBottomupClustering {

	DistanceCache cache;
	DistanceMeasure measure;
	Map<Integer, List<Integer>> clusters;
	List<Vector> points;
	int mergingClusterId, mergedClusterId;
	final int numCore = Runtime.getRuntime().availableProcessors();

	public CachedBottomupClustering(List<Vector> points, DistanceMeasure measure, long memoryCapacity) throws Exception {
		if (memoryCapacity < 0) {
			System.err.println("Cannot cache distance!: " + memoryCapacity);
		}
		this.measure = measure;
		this.points = points;
		initClusters(points);
		cache = new DistanceCache(points, measure, memoryCapacity - Integer.SIZE / 8 * points.size());
	}

	public void waitAll(Thread [] t) throws InterruptedException {
		for (int n = 0; n < t.length; n++) {
			t[n].join();
		}
	}

	public void initClusters(List<Vector> idPoints) throws InterruptedException {
		final List<Vector> idPoints_ = idPoints;
		Thread [] t = new Thread[numCore];
		for (int n_ = 0; n_ < numCore ; n_++) {
			final int n = n_;
			t[n] = new Thread() {
				public void run() {
					for (int i = n; i < idPoints_.size(); i+= numCore)  {
						List<Integer> clusteredPoints = new ArrayList<Integer>();
						clusteredPoints.add(i);
						clusters.put(i, clusteredPoints);
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
	}

	public double getScore(int cluster1, int cluster2) {
		double d = 0;
		for (int p: clusters.get(cluster1)) {
			for (int p2: clusters.get(cluster2)) {
				d += cache.get(p, p2);
			}
		}
		return d / clusters.get(cluster1).size() / clusters.get(cluster2).size();
	}

	public boolean next() {
		final double [] min_d_ = new double[numCore];
		final int [] min_i_ = new int[numCore];
		final int [] min_j_ = new int[numCore];
		Thread [] t = new Thread[numCore];

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
							double d = getScore(i, j);
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

		mergingClusterId = min_j;
		mergedClusterId = min_i;

		return true;
	}

	public void update() {
		clusters.get(mergingClusterId).addAll(clusters.get(mergedClusterId));
		clusters.remove(mergedClusterId);
	}

	public int mergingClusterId() {
		return mergingClusterId;
	}

	public int mergedClusterId() {
		return mergedClusterId;
	}

	public DistanceMeasure getDistanceMeasure() {
		return measure;
	}
}
