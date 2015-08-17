package ac.keio.sslab.clustering.bottomup;

import java.util.TreeMap;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

public class OrderCache {

	TreeMap<Double, int[]> simOrder;
	int maxSimOrderSize;

	public OrderCache(long memoryCapacity, long numPoints) {
		this.simOrder = Maps.newTreeMap();

		long usedMemory = 0;
		maxSimOrderSize = 0;
		long numAllCombinations = numPoints * (numPoints - 1) / 2; // = 1 + 2 + 3 + ,.. + {points.size() - 1}
		while (maxSimOrderSize < 100 && maxSimOrderSize <= numAllCombinations && usedMemory + (Double.SIZE + Integer.SIZE * 2) / 8 < memoryCapacity) {
			usedMemory += (Double.SIZE + Integer.SIZE * 2) / 8; // simOrder + revSimOrder
			++maxSimOrderSize;
		}
		System.out.println("maxSimOrderSize: " + maxSimOrderSize);
	}

	public void push(int cluster1, int cluster2, double s) {
		if (simOrder.size() + 1 >= maxSimOrderSize) {
			// push only if the similarity is bigger than the last cached entry
			if (s < simOrder.lastKey()) {
				simOrder.remove(simOrder.lastKey()); // there might be more than two clusters but we ignore!
			} else {
				return;
			}
		}

		int [] sameSimClusterPairs = simOrder.put(s, new int[] {cluster1, cluster2});

		if (sameSimClusterPairs != null) {
			// there might be same distance pairs
			int [] newPair = new int[sameSimClusterPairs.length + 2];
			System.arraycopy(sameSimClusterPairs, 0, newPair, 0, sameSimClusterPairs.length);
			newPair[newPair.length - 2] = cluster1;
			newPair[newPair.length - 1] = cluster2;
			simOrder.put(s, newPair);
		}
	}

	public Entry<Double, int[]> top() {
		return simOrder.firstEntry();
	}

	public void pop() {
		double s = simOrder.firstKey();
		int [] maxPair = simOrder.remove(s);
		// temporally, revSimOrder for maxPair[0] & maxPair[1] with other clusters get invalid. this is for parallelizing invalidate()

		if (maxPair.length > 2) {
			// there might be same distance pairs
			int[] newPair = new int[maxPair.length - 2];
			System.arraycopy(maxPair, 2, newPair, 0, maxPair.length - 2);
			simOrder.put(s, newPair);
		}
	}

	// must be called right after pop()
	public void invalidate(int cluster1, int cluster2) {
		__invalidate(cluster1);
		__invalidate(cluster2);
	}

	protected void __invalidate(int cluster) {
		for (java.util.Iterator<Entry<Double, int[]>> i = simOrder.entrySet().iterator(); i.hasNext();) {
			Entry<Double, int[]> e = i.next();
			int [] clusters = e.getValue();
			boolean shouldInvalidate = false;
			for (int c: clusters)  {
				if (c == cluster) {
					shouldInvalidate = true;
					break;
				}
			}
			if (!shouldInvalidate) {
				continue;
			}

			if (clusters.length == 2) {
				i.remove();
				continue;
			}

			// rarely go into here
			int [] newPair = new int[clusters.length - 2];
			int c = 0;
			for (int j = 0; j < clusters.length; j += 2) {
				if (cluster != clusters[j] && cluster != clusters[j + 1]) {
					newPair[c++] = clusters[j];
					newPair[c++] = clusters[j + 1];
				}
			}
			simOrder.put(e.getKey(), newPair);
		}
	}

	public boolean shouldUpdate() {
		return simOrder.isEmpty();
	}

	public void clear() {
		simOrder.clear();
	}

	public void dump() {
		for (Entry<Double, int[]> e: simOrder.entrySet()) {
			System.out.print(e.getKey());
			for (int c: e.getValue()) {
				System.out.print("," + c);
			}
			System.out.print(" / ");
		}
		System.out.println();
	}
}
