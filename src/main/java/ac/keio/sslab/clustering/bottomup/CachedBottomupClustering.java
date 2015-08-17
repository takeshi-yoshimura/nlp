package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.utils.ArrayMap;

public class CachedBottomupClustering {

	public final int numCore = Runtime.getRuntime().availableProcessors();
	double [][] distance;
	// per thread data
	// Note: elements for same cluster ID belongs to same per thread region key
	// e.g., stats for cluster ID == 1 appear in order.get(1) and clusters.getKey(1)
	List<OrderCache> order;
	ArrayMap<Integer, Integer> clusters; // Multimap cannot be used due to thread-safety

	DistanceMeasure measure;
	List<Vector> points;

	public void waitAll(Thread [] list) throws InterruptedException {
		for (Thread t: list) {
			t.join();
		}
	}

	public CachedBottomupClustering(List<Vector> points, DistanceMeasure measure, long memoryCapacity) throws Exception {
		this.measure = measure;
		this.points = points;

		long usedMemory = points.size() * Integer.SIZE / 8 * 2; //clusters

		System.out.print("allocate memory for distance...");
		this.distance = new double[points.size()][];
		for (int i = 0; i < points.size(); i++) {
			distance[i] = new double[i + 1];
			usedMemory += Double.SIZE / 8 * (i + 1);
		}
		System.out.println(" finished (used roughly: " + usedMemory + " bytes)");

		this.order = new ArrayList<OrderCache>();
		int numCore = Runtime.getRuntime().availableProcessors();
		for (int n = 0; n < numCore; n++) {
			this.order.add(new OrderCache((memoryCapacity - usedMemory) / numCore, points.size()));
		}
		this.clusters = ArrayMap.newMap(numCore);
		System.out.print("allocate clusters and order caches initially...");
		init();
		System.out.println(" finished");
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
							distance[i][j] = measure.distance(points.get(i + 1), points.get(j));
							order.get(N).push(i + 1, j, distance[i][j]);
						}
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
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
			if (!clusters.contains(e.getValue()[0]) || !clusters.contains(e.getValue()[1])) {
				System.err.println("what?");;
			}
			if (minS > e.getKey()) {
				minS = e.getKey();
				minPair = e.getValue();
				minN = n;
			} else if (minS == e.getKey() && minPair[0] < e.getValue()[0]) {
				minS = e.getKey();
				minPair = e.getValue();
				minN = n;
			}
		}
		if (minPair == null) {
			return null;
		}

		// merge the most similar clusters
		clusters.addAlltoKey(minPair[0], clusters.values(minPair[1]));
		clusters.removeKey(minPair[1]);

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
					order.get(N).invalidate(cluster1, cluster2);
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
					int i = newCluster + 1;
					while (i % numCore != N) {
						i++;
					}
					for (; i < points.size(); i += numCore)  {
						if (!clusters.contains(N, i)) {
							continue;
						}
						order.get(N).push(i, newCluster, getSimilarity(i, newCluster));
					}
					if (newCluster % numCore == N) {
						for (i = 0; i < newCluster; i++)  {
							if (!clusters.contains(i)) {
								continue;
							}
							order.get(N).push(newCluster, i, getSimilarity(newCluster, i));
						}
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
	}

	protected void renewOrder() throws InterruptedException {
		Thread [] t = new Thread[numCore];
		for (int n = 0; n < numCore; n++) {
			final int N = n;
			t[n] = new Thread() {
				public void run() {
					order.get(N).clear();
					for (int i = N; i < points.size(); i += numCore)  {
						if (!clusters.contains(N, i)) {
							continue;
						}
						for (int j = 0; j < i; j++) {
							if (!clusters.contains(j)) {
								continue;
							}
							order.get(N).push(i, j, getSimilarity(i, j));
						}
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
	}

	// use Group Average as cluster similarity measure
	protected double getSimilarity(int cluster1, int cluster2) {
		double d = 0;
		for (int p1: clusters.values(cluster1)) {
			for (int p2: clusters.values(cluster2)) {
				d += getPointDistance(p1, p2);
			}
		}
		return d / clusters.values(cluster1).size() / clusters.values(cluster2).size();
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
}
