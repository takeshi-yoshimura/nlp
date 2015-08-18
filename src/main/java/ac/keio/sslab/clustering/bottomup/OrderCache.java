package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

public class OrderCache {

	TreeMap<Double, int[]> simOrder;
	int maxSimOrderSize;

	public OrderCache(int cacheEntrySize) {
		this.simOrder = Maps.newTreeMap();
		maxSimOrderSize = cacheEntrySize;
	}

	public OrderCache() {
		this(100);
	}

	public void pushIfMoreSimilar(int cluster1, int cluster2, double s) {
		if (s < simOrder.lastKey()) {
			push(cluster1, cluster2, s);
		}
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
	protected void invalidate(int cluster) {
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
			List<Integer> newArray = new ArrayList<Integer>();
			for (int j = 0; j < clusters.length; j += 2) {
				if (cluster != clusters[j] && cluster != clusters[j + 1]) {
					newArray.add(clusters[j]);
					newArray.add(clusters[j + 1]);
				}
			}
			if (newArray.isEmpty()) {
				i.remove();
				continue;
			}
			int [] newPair = new int[newArray.size()];
			int l = 0;
			for (int k: newArray) {
				newPair[l++] = k;
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

	public String dump(boolean displaySimilarity) {
		StringBuilder sb = new StringBuilder();
		for (Entry<Double, int[]> e: simOrder.entrySet()) {
			for (int c: e.getValue()) {
				sb.append(c).append(',');
			}
			sb.setLength(sb.length() - 1);
			if (displaySimilarity) {
				sb.append('(').append(e.getKey()).append(')');
			}
			sb.append(" / ");
		}
		return sb.toString();
	}
}
