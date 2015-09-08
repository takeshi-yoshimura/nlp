package ac.keio.sslab.nlp;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.mahout.math.Vector;

import ac.keio.sslab.clustering.bottomup.IndexBottomupClusteringRunner;
import ac.keio.sslab.clustering.view.HierarchicalCluster;

public class DimensionBottomUpJob extends BottomUpJob {

	@Override
	public String getAlgorithmName() {
		return "clusgtering.dimbottomup";
	}

	@Override
	public String getJobDescription() {
		return "group average hierarchical clustering for each dimension";
	}

	@Override
	public void runClustering(List<Vector> points, Map<Integer, HierarchicalCluster> clusters, Map<Integer, String> topicStr, File localOutputDir) throws Exception {
		File dimensionFile = new File(localOutputDir, "dimensionCluster");
		int numCore = Runtime.getRuntime().availableProcessors();
		Thread t [] = new Thread[numCore];
		int tIndex [] = new int[numCore];
		int i = 0;
		for (int n = 0; n < numCore; n++) {
			System.out.println("Start: " + i);
			t[n] = new IndexBottomupClusteringRunner(points, i, new File(dimensionFile, i + ".csv"), topicStr.get(i), clusters);
			tIndex[n] = i;
			i++;
			t[n].start();
		}
		while (i < points.get(0).size()) {
			Thread.sleep(1000);
			for (int n = 0; n < numCore; n++) {
				if (!t[n].isAlive()) {
					System.out.println("Finished: " + tIndex[n]);
					System.out.println("Start: " + i);
					t[n] = new IndexBottomupClusteringRunner(points, i, new File(dimensionFile, i + ".csv"), topicStr.get(i), clusters);
					tIndex[n] = i;
					i++;
					t[n].start();
				}
			}
		}
		for (int n = 0; n < numCore; n++) {
			if (t[n].isAlive()) {
				t[n].join();
				System.out.println("Finished: " + tIndex[n]);
			}
		}
	}
}
