package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;

public class BottomupClustering {

	public static void run(Path input, FileSystem fs, File output, boolean doForceWrite, Map<Integer, String> topicStr) throws Exception {
		List<Vector> points = new ArrayList<Vector>();
		Map<Integer, Integer> pointIndex = new HashMap<Integer, Integer>();
		Map<Integer, HierarchicalCluster> clusters = new HashMap<Integer, HierarchicalCluster>();

		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, fs, Integer.class, Vector.class);
		int nextClusterID = 0;
		while (reader.seekNext()) {
			pointIndex.put(points.size(), reader.key());
			HierarchicalCluster c = new HierarchicalCluster(points.size(), nextClusterID++);
			clusters.put(c.getID(), c);
			points.add(reader.val());
			c.setCentroid(points, topicStr);
			c.setDensity(1.0);
		}
		reader.close();
		CachedBottomupClustering clustering = new CachedBottomupClustering(points);

		PrintWriter writer = JobUtils.getPrintWriter(output);
		writer.println("#HierarchicalClusterID,size,density,leftCID,rightCID,centroid...,pointIDs...");
		int i = 0;
		int [] nextPair = null;
		HierarchicalCluster newC = null;
		while((nextPair = clustering.popMostSimilarClusterPair()) != null) {
			int merging = pointIndex.get(nextPair[0]);
			int merged = pointIndex.get(nextPair[1]);
			double similarity = clustering.getMaxSimilarity();
			System.out.println("Iteration #" + i++ + ": " + merging + "," + merged);

			HierarchicalCluster leftC = clusters.get(merging);
			HierarchicalCluster rightC = clusters.get(merged);
			newC = new HierarchicalCluster(leftC, rightC, nextClusterID++);
			newC.setDensity(similarity);
			newC.setCentroid(points, topicStr);
			clusters.put(merging, newC);
			clusters.remove(merged);

			writer.println(leftC.toString());
			writer.println(rightC.toString());
			writer.flush();
		}
		writer.println(newC.toString());

		writer.close();
	}
}
