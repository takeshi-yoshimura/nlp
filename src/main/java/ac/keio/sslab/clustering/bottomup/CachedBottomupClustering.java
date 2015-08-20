package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.utils.ArrayMap;

public class CachedBottomupClustering {

	public final int numCore = Runtime.getRuntime().availableProcessors();
	double [][] distance;

	// per thread data
	List<OrderCache> order;
	ArrayMap<Integer, Integer> clusters; // Multimap cannot be used due to thread-safety

	List<Vector> points;
	protected double currentMinS;
	protected List<Integer> newCluster;

	public void waitAll(Thread [] list) throws InterruptedException {
		for (Thread t: list) {
			t.join();
		}
	}

	public CachedBottomupClustering(List<Vector> points) throws Exception {
		this.points = points;

		this.distance = new double[points.size()][];
		for (int i = 0; i < points.size(); i++) {
			distance[i] = new double[i + 1];
		}

		this.order = new ArrayList<OrderCache>();
		for (int n = 0; n < numCore; n++) {
			this.order.add(new OrderCache());
		}
		this.clusters = ArrayMap.newMap(numCore);
		init();
	}

	protected void init() throws InterruptedException {
		Thread [] t = new Thread[numCore];
		for (int n = 0; n < numCore; n++) {
			final int N = n;
			t[n] = new Thread() {
				public void run() { // no races!
					for (int i = N; i < points.size(); i += numCore)  {
						clusters.init(N, i, i);
						if (i == points.size() - 1) {
							break;
						}
						for (int j = 0; j <= i; j++) {
							distance[i][j] = getDistance(points.get(i + 1), points.get(j));
							order.get(N).push(i + 1, j, getInitialSimilarity(distance[i][j]));
						}
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
	}

	protected double getDistance(Vector v1, Vector v2) {
		return getHastiaDistance(v1, v2);
	}

	protected double getInitialSimilarity(double d) {
		return getHastiaInitSimilarity(d);
	}

	DistanceMeasure measure = new EuclideanDistanceMeasure();
	protected double getHastiaDistance(Vector v1, Vector v2) {
		return measure.distance(v1, v2);
	}

	protected double getHastiaInitSimilarity(double d) {
		return d;
	}

	protected double getManningDistance(Vector v1, Vector v2) {
		double d = 0;
		for (int k = 0; k < v1.size(); k++) {
			d += v1.get(k) * v2.get(k);
		}
		return d;
	}

	protected double getManningDistance(double d) {
		return d / 2;
	}

	public int[] popMostSimilarClusterPair() throws Exception {
		double minS = Double.MAX_VALUE;
		int minN = -1;
		int [] minPair = null;
		for (int n = 0; n < order.size(); n++) {
			Entry<Double, int[]> e = order.get(n).top();
			if (e == null) {
				continue; // go into here if iterations for a thread finished
			}
			if (minS > e.getKey()) {
				minS = e.getKey();
				minPair = e.getValue();
				minN = n;
			} else if (minS == e.getKey()) {
				if (minPair[0] > e.getValue()[0] || (minPair[0] == e.getValue()[0] && minPair[1] > e.getValue()[1])) {
					minS = e.getKey();
					minPair = e.getValue();
					minN = n;
				}
			}
		}
		if (minPair == null) {
			return null;
		}
		currentMinS = minS;

		// merge the most similar clusters
		clusters.addAlltoKey(minPair[0], clusters.values(minPair[1]));
		clusters.removeKey(minPair[1]);
		newCluster = clusters.values(minPair[0]);

		// update cache in inconsistent manners: remove most similar clusters' order caches, & update order for the new cluster
		order.get(minN).pop();
		invalidateOrder(minPair[0], minPair[1]);

		boolean shouldRenew = false;
		for (int n = 0; n < numCore; n++) {
			if (order.get(n).shouldUpdate()) {
				shouldRenew = true;
				break;
			}
		}
		if (shouldRenew) {
			renewOrder();
		} else {
			updateOrder(minPair[0]);
		}
		return minPair;
	}

	protected void invalidateOrder(final int cluster1, final int cluster2) throws InterruptedException {
		Thread [] t = new Thread[numCore];
		for (int n = 0; n < numCore; n++) {
			final int N = n;
			t[n] = new Thread() {
				public void run() {
					// at most two threads go into O(size of cache) iterations but we cannot balance this because ArrayMap allows thread-safe access only with keys
					// synchronized Map can avoid the issue but I don't think that will scale. Also, the size of cache is small enough, right?
					order.get(N).invalidate(cluster1);
					order.get(N).invalidate(cluster2);
				}
			};
			t[n].start();
		}
		waitAll(t);
	}

	protected void updateOrder(final int newCluster) throws InterruptedException {
		Thread [] t = new Thread[numCore];
		for (int n = 0; n < numCore; n++) {
			final int N = n;
			t[n] = new Thread() {
				public void run() {
					int c = 0;
					for (int d = 0; d < numCore; d++) {
						for (int i: clusters.keySet(d)) {
							if (c++ % numCore != N) {
								continue;
							}
							if (newCluster < i) {
								order.get(N).pushIfMoreSimilar(i, newCluster, getSimilarity(i, newCluster));
							} else if (i < newCluster) {
								order.get(N).pushIfMoreSimilar(newCluster, i, getSimilarity(newCluster, i));
							}
						}
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
	}

	protected void renewOrder() throws InterruptedException {
		final List<Integer> clusterIDs = new ArrayList<Integer>();
		for (int d = 0; d < numCore; d++) {
			order.get(d).clear();
			clusterIDs.addAll(clusters.keySet(d));
		}
		Collections.sort(clusterIDs);
		Thread [] t = new Thread[numCore];
		for (int n = 0; n < numCore; n++) {
			final int N = n;
			t[n] = new Thread() {
				public void run() {
					int c = 0;
					for (int i: clusterIDs) {
						for (int j: clusterIDs) {
							if (i <= j) {
								break;
							}
							if (c++ % numCore == N) {
								order.get(N).push(i, j, getSimilarity(i, j));
							}
						}
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
	}

	protected double getSimilarity(int cluster1, int cluster2) {
		return getHastieSimilarity(cluster1, cluster2);
	}

	protected double getHastieSimilarity(int cluster1, int cluster2) {
		double d = 0;
		for (int p1: clusters.values(cluster1)) {
			for (int p2: clusters.values(cluster2)) {
				d += getPointDistance(p1, p2);
			}
		}
		return d / clusters.values(cluster1).size() / clusters.values(cluster2).size();
	}

	// use Group Average as cluster similarity measure
	protected double getManningSimilarity(int cluster1, int cluster2) {
		double d = 0;
		for (int p1: clusters.values(cluster1)) {
			for (int p2: clusters.values(cluster2)) {
				d += getPointDistance(p1, p2);
			}
		}
		int N1 = clusters.values(cluster1).size();
		int N2 = clusters.values(cluster2).size();
		return d / (N1 + N2) / (N1 + N2 - 1);
	}

	protected double getPointDistance(int point1, int point2) {
		if (point1 < point2) {
			int tmp = point2;
			point2 = point1;
			point1 = tmp;
		}

		return distance[point1 - 1][point2];
	}

	public Map<Integer, List<Integer>> getClusters() {
		Map<Integer, List<Integer>> ret = new HashMap<Integer, List<Integer>>();
		for (int n = 0; n < numCore; n++) {
			for (Entry<Integer, List<Integer>> e: clusters.entrySet(n)) {
				ret.put(e.getKey(), e.getValue());
			}
		}
		return ret;
	}

	public double getMaxSimilarity() {
		return currentMinS;
	}

	public List<Integer> getNewClusteredPoints() {
		return newCluster;
	}
}