package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;

public class CachedBottomupClustering {

	final int numCore = Runtime.getRuntime().availableProcessors();
	Thread [] t = new Thread[numCore];

	// {point id, point}
	double [][] distance;
	// per thread data
	List<Map<Integer, List<Integer>>> clusters;
	List<Multimap<Integer, Double>> revSimOrder;
	List<TreeMultimap<Double, int[]>> simOrder;
	int maxSimOrderSize;

	DistanceMeasure measure;
	List<Vector> points;

	public CachedBottomupClustering(List<Vector> points, DistanceMeasure measure, long memoryCapacity) throws InterruptedException {
		this.measure = measure;
		this.points = points;

		this.simOrder = new ArrayList<TreeMultimap<Double, int[]>>();
		this.revSimOrder = new ArrayList<Multimap<Integer, Double>>();
		for (int i = 0; i < numCore; i++) {
			Multimap<Integer, Double> revSimOrderLocal = ArrayListMultimap.create();
			TreeMultimap<Double, int[]> simOrderLocal = TreeMultimap.create(Ordering.natural(), Ordering.arbitrary());
			this.simOrder.add(simOrderLocal);
			this.revSimOrder.add(revSimOrderLocal);
		}

		initClusters();
		initDistance(memoryCapacity);
	}

	protected void waitAll(Thread [] t) throws InterruptedException {
		for (int n = 0; n < t.length; n++) {
			t[n].join();
		}
	}

	protected void initClusters() throws InterruptedException {
		this.clusters = new ArrayList<>();
		for (int i = 0; i < numCore; i++) {
			this.clusters.add(new HashMap<Integer, List<Integer>>());
		}
		Thread [] t = new Thread[numCore];
		for (int n_ = 0; n_ < numCore ; n_++) {
			final int n = n_;
			t[n] = new Thread() {
				public void run() {
					Map<Integer, List<Integer>> localMap = clusters.get(n);
					for (int i = n; i < points.size(); i+= numCore)  {
						List<Integer> clusteredPoints = new ArrayList<Integer>();
						clusteredPoints.add(i);
						localMap.put(i, clusteredPoints); // always no collisions
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
	}

	protected void initDistance(long memoryCapacity) throws InterruptedException {
		int rowEnd_ = 0;
		long usedMemory = 0;
		while (rowEnd_ < points.size() && usedMemory + Double.SIZE / 8 * (rowEnd_ + 1) < memoryCapacity) {
			usedMemory += Double.SIZE / 8 * ++rowEnd_;
		}

		maxSimOrderSize = 0;
		while (usedMemory + (Double.SIZE + Integer.SIZE * 2) / 8 * (maxSimOrderSize + 1) < memoryCapacity) {
			usedMemory += (Double.SIZE + Integer.SIZE * 2) / 8 * ++maxSimOrderSize;
		}

		for (int n_ = 0; n_ < numCore; n_++) {
			final int n = n_;
			final int rowEnd = rowEnd_;
			t[n] = new Thread() {
				public void run() {
					TreeMultimap<Double, int[]> simOrderLocal = simOrder.get(n);
					Multimap<Integer, Double> revSimOrderLocal = revSimOrder.get(n);
					for (int i = n; i < points.size() - 1 && i < rowEnd; i += numCore) {
						distance[i] = new double[i + 1];
						for (int j = 0; j <= i; j++) {
							distance[i][j] = getDistanceNaive(i + 1, j);

							if (simOrderLocal.size() > maxSimOrderSize) {
								if (distance[i][j] < simOrderLocal.asMap().lastKey()) {
									Entry<Double, Collection<int[]>> e = simOrderLocal.asMap().lastEntry();
									int [] a = e.getValue().iterator().next();
									simOrderLocal.remove(e.getKey(), a);
									revSimOrderLocal.remove(a[0], e.getKey());
									revSimOrderLocal.remove(a[1], e.getKey());
								} else {
									continue;
								}
							}
							simOrderLocal.put(distance[i][j], new int[] {i + 1, j});
							revSimOrderLocal.put(i + 1, distance[i][j]);
							revSimOrderLocal.put(j, distance[i][j]);
						}
					}
				}
			};
			t[n].start();
		}

		waitAll(t);
	}

	protected double getDistance(int point1, int point2) {
		if (point1 < point2) {
			int tmp = point2;
			point2 = point1;
			point1 = tmp;
		}

		if (point1 - 1 < distance.length) {
			return distance[point1 - 1][point2];
		}

		return getDistanceNaive(point1, point2);
	}

	protected double getDistanceNaive(int point1, int point2) {
		return measure.distance(points.get(point1), points.get(point2));
	}

	protected double getSimilarityNaive(int cluster1, int cluster2) {
		int n1 = 0, n2 = 0;
		while (!clusters.get(n1++).containsKey(cluster1) && n1 < numCore);
		while (!clusters.get(n2++).containsKey(cluster2) && n2 < numCore);

		double d = 0;
		for (int p1: clusters.get(n1).get(cluster1)) {
			for (int p2: clusters.get(n2).get(cluster2)) {
				d += getDistance(p1, p2);
			}
		}
		return d / clusters.get(n1).get(cluster1).size() / clusters.get(n2).get(cluster2).size();
	}

	public int[] popMostSimilarClusterPair() throws InterruptedException {
		int [] maxPair = null;
		int maxThread = -1;
		double maxSim = Double.MAX_VALUE;
		for (int n = 0; n < numCore; n++) {
			TreeMultimap<Double, int[]> simOrderLocal = simOrder.get(n);
			Entry<Double, Collection<int[]>> e = simOrderLocal.asMap().firstEntry();
			if (maxSim > e.getKey()) { // max similarity means minimal value in simOrder
				maxSim = e.getKey();
				maxPair = e.getValue().iterator().next();
				maxThread = n;
			}
		}

		if (maxPair == null) {
			return null;
		}

		simOrder.get(maxThread).remove(maxSim, maxPair);

		int n1_ = 0, n2_ = 0;
		while (clusters.get(n1_++).containsKey(maxPair[0]) && n1_ < numCore);
		while (clusters.get(n2_++).containsKey(maxPair[1]) && n2_ < numCore);

		clusters.get(n1_).get(maxPair[0]).addAll(clusters.get(n2_).get(maxPair[1]));
		clusters.get(n2_).remove(maxPair[1]);

		final int n1 = n1_, n2 = n2_;
		for (int n_ = 0; n_ < numCore; n_++) {
			final int n = n_;
			final int cluster1 = maxPair[0], cluster2 = maxPair[1];
			t[n] = new Thread() {
				public void run() {
					for (double d: revSimOrder.get(n).get(cluster1)) {
						for (Iterator<int[]> i = simOrder.get(n1).get(d).iterator(); i.hasNext();) {
							int cand[] = i.next();
							if (cand[0] == cluster1 || cand[1] == cluster1) {
								i.remove();
							}
						}
					}
					for (double d: revSimOrder.get(n).get(cluster2)) {
						for (Iterator<int[]> i = simOrder.get(n2).get(d).iterator(); i.hasNext();) {
							int cand[] = i.next();
							if (cand[0] == cluster2 || cand[1] == cluster2) {
								i.remove();
							}
						}
					}
					revSimOrder.get(n).removeAll(cluster1);
					revSimOrder.get(n).removeAll(cluster2);
				}
			};
			t[n].start();
		}
		waitAll(t);

		update(maxPair[0]);
		return maxPair;
	}

	protected void update(final int newCluster) throws InterruptedException {
		for (int n_ = 0; n_ < numCore; n_++) {
			final int n = n_;
			t[n] = new Thread() {
				public void run() {
					TreeMultimap<Double, int[]> simOrderLocal = simOrder.get(n);
					Multimap<Integer, Double> revSimOrderLocal = revSimOrder.get(n);
					for (int j: clusters.get(n).keySet()) {
						if (j == newCluster) {
							continue;
						}
						double newSim = getSimilarityNaive(newCluster, j);
						if (newSim > simOrderLocal.asMap().lastKey()) {
							continue;
						} else if (simOrderLocal.size() > maxSimOrderSize) {
							Entry<Double, Collection<int[]>> e = simOrderLocal.asMap().lastEntry();
							int [] a = e.getValue().iterator().next();
							simOrderLocal.remove(e.getKey(), a);
							revSimOrderLocal.remove(a[0], e.getKey());
							revSimOrderLocal.remove(a[1], e.getKey());
						}
						simOrderLocal.put(newSim, new int[] {newCluster, j});
						revSimOrderLocal.put(newCluster, newSim);
						revSimOrderLocal.put(j, newSim);
					}
				}
			};
			t[n].start();
		}
		waitAll(t);

		boolean shouldRecal = false;
		for (int n = 0; n < numCore; n++) {
			if (simOrder.get(n).size() < maxSimOrderSize / 2) {
				shouldRecal = true;
				break;
			}
		}
		if (!shouldRecal) {
			return;
		}

		for (int n_ = 0; n_ < numCore; n_++) {
			final int n = n_;

			t[n] = new Thread() {
				public void run() {
					TreeMultimap<Double, int[]> simOrderLocal = simOrder.get(n);
					Multimap<Integer, Double> revSimOrderLocal = revSimOrder.get(n);
					for (int i: clusters.get(n).keySet()) {
						for (int j = 0; j < i; j++) {
							if (!clusters.contains(j)) {
								continue;
							}

							double s = getSimilarityNaive(i, j);

							if (simOrderLocal.size() > maxSimOrderSize) {
								if (distance[i][j] < simOrderLocal.asMap().lastKey()) {
									Entry<Double, Collection<int[]>> e = simOrderLocal.asMap().lastEntry();
									int [] a = e.getValue().iterator().next();
									simOrderLocal.remove(e.getKey(), a);
									revSimOrderLocal.remove(a[0], e.getKey());
									revSimOrderLocal.remove(a[1], e.getKey());
								} else {
									continue;
								}
							}
							simOrderLocal.put(s, new int[] {i, j});
							revSimOrderLocal.put(i, s);
							revSimOrderLocal.put(j, s);
						}
					}
				}
			};
			t[n].start();
		}
		waitAll(t);
	}
}
