package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

public class HierarchicalClusterPrioritizer {

	List<HierarchicalCluster> clusters;
	TreeMap<Double, HierarchicalCluster> order;
	int maxSize;

	public HierarchicalClusterPrioritizer(File outputFile) throws IOException {
		clusters = HierarchicalClusterGraph.parseResult(outputFile).getClusters();
		order = new TreeMap<Double, HierarchicalCluster>(Collections.reverseOrder());
		maxSize = Integer.MIN_VALUE;
		for (HierarchicalCluster c: clusters) {
			if (maxSize < c.size()) {
				maxSize = c.size();
			}
		}
		for (HierarchicalCluster c: clusters) {
			if (c.getLeft() == null || c.getRight() == null) {
				continue;
			}
			order.put(score(c, c.getLeft()), c.getLeft());
			order.put(score(c, c.getRight()), c.getRight());
		}
	}

	public double score(HierarchicalCluster c, HierarchicalCluster child) {
		if (child.getDensity() == 0.0) {
			return Double.MIN_VALUE;
		}
		return (double)c.size() / maxSize * (child.getDensity() - c.getDensity()) / child.getDensity();
	}

	public TreeMap<Double, HierarchicalCluster> getOrder() {
		return order;
	}

	public int getMaxSize() {
		return maxSize;
	}
}