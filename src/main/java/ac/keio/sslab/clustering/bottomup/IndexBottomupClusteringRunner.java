package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.math.Vector;

import ac.keio.sslab.analytics.HierarchicalCluster;
import ac.keio.sslab.nlp.job.JobUtils;

public class IndexBottomupClusteringRunner extends Thread {

	int index;
	File clustersFile;
	String topicStr;
	List<Vector> points;
	Map<Integer, HierarchicalCluster> clusters;
	IndexBottomupClustering clustering;

	public IndexBottomupClusteringRunner(List<Vector> points, int index, File clustersFile, String topicStr, Map<Integer, HierarchicalCluster> initialCluster) {
		this.points = points;
		this.index = index;
		this.clustersFile = clustersFile;
		this.topicStr = topicStr;
		this.clusters = new HashMap<Integer, HierarchicalCluster>(initialCluster);
		this.clustering = new IndexBottomupClustering(points, index);
	}

	@Override
	public void run() {
		try {
			int nextClusterID = points.size();

			PrintWriter writer = JobUtils.getPrintWriter(clustersFile);
			writer.println("#HierarchicalClusterID,size,density,parentID,leftCID,rightCID,centroid...,pointIDs...");
			int i = 0;
			int [] nextPair = null;
			HierarchicalCluster newC = null;
			while((nextPair = clustering.popMostSimilarClusterPair()) != null) {
				double similarity = clustering.getMaxSimilarity();
				System.out.println("@" + index + " Iteration #" + i++ + ": " + nextPair[0] + "," + nextPair[1]);

				HierarchicalCluster leftC = clusters.get(nextPair[0]);
				HierarchicalCluster rightC = clusters.get(nextPair[1]);
				newC = new HierarchicalCluster(leftC, rightC, nextClusterID++);
				newC.setDensity(similarity);
				Map<String, Double> cent = new HashMap<String, Double>();
				double d = 0;
				for (int pointID: newC.getPoints()) {
					d += points.get(pointID).get(index);
				}
				cent.put(topicStr, d / newC.size());
				newC.setCentroid(cent);
				clusters.put(nextPair[0], newC);
				clusters.remove(nextPair[1]);

				writer.println(leftC.toString());
				writer.println(rightC.toString());
				writer.flush();
			}
			writer.println(newC.toString());
			writer.close();
			System.out.println("@" + index + " Finished!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
