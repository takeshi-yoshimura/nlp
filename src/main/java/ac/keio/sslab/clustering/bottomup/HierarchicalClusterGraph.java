package ac.keio.sslab.clustering.bottomup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HierarchicalClusterGraph {

	List<HierarchicalCluster> clusters;
	HierarchicalCluster root;

	protected HierarchicalClusterGraph(List<HierarchicalCluster> clusters, HierarchicalCluster root) {
		this.clusters = clusters;
		this.root = root;
	}

	public static HierarchicalClusterGraph parseResult(File outputFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(outputFile));
		String line = null;
		Map<Integer, HierarchicalCluster> graph = new HashMap<Integer, HierarchicalCluster>();
		List<HierarchicalCluster> clusters = new ArrayList<HierarchicalCluster>();
		Set<Integer> HierarchicalClusterIDs = new HashSet<Integer>();
		while ((line = reader.readLine()) != null) {
			if (line.startsWith("#")) {
				continue;
			}
			HierarchicalCluster c = HierarchicalCluster.parseString(line);
			clusters.add(c);
			graph.put(c.getID(), c);
			HierarchicalClusterIDs.add(c.getID());
		}
		reader.close();

		for (HierarchicalCluster c: clusters) {
			if (c.getLeft() == null || c.getRight() == null) {
				continue;
			}
			c.setLeft(graph.get(c.getLeft().getID()));
			c.setRight(graph.get(c.getRight().getID()));
			HierarchicalClusterIDs.remove(c.getLeft().getID());
			HierarchicalClusterIDs.remove(c.getRight().getID());
		}
		HierarchicalCluster root = graph.get(HierarchicalClusterIDs.iterator().next());

		return new HierarchicalClusterGraph(clusters, root);
	}

	public HierarchicalCluster getRoot() {
		return root;
	}

	public List<HierarchicalCluster> getClusters() {
		return clusters;
	}
}
