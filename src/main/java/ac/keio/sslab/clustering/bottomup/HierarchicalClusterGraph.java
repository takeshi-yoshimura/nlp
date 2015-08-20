package ac.keio.sslab.clustering.bottomup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;

public class HierarchicalClusterGraph {

	List<Vector> pointVectors;
	Map<Integer, Integer> pointIndex;
	List<HierarchicalCluster> HierarchicalClusters;
	HierarchicalCluster root;

	protected HierarchicalClusterGraph(List<Vector> pointVectors, Map<Integer, Integer> pointIndex, List<HierarchicalCluster> HierarchicalClusters, HierarchicalCluster root) {
		this.pointVectors = pointVectors;
		this.pointIndex = pointIndex;
		this.HierarchicalClusters = HierarchicalClusters;
		this.root = root;
	}

	public static HierarchicalClusterGraph parseMergingMergedFile(File input, Path pointInput, FileSystem fs) throws IOException {
		SequenceDirectoryReader<Integer, Vector> pointReader = new SequenceDirectoryReader<Integer, Vector>(pointInput, fs, Integer.class, Vector.class);
		int HierarchicalClusterID = 0;
		List<HierarchicalCluster> HierarchicalClusters = new ArrayList<HierarchicalCluster>();
		Map<Integer, Integer> pointIndex = new HashMap<Integer, Integer>();
		List<Vector> pointVectors = new ArrayList<Vector>();
		Map<Integer, HierarchicalCluster> graph = new HashMap<Integer, HierarchicalCluster>();
		while (pointReader.seekNext()) {
			pointIndex.put(pointVectors.size(), pointReader.key());
			pointVectors.add(pointReader.val());
			HierarchicalCluster c = new HierarchicalCluster(pointReader.key(), HierarchicalClusterID++);
			graph.put(pointReader.key(), c);
			HierarchicalClusters.add(c);
		}
		pointReader.close();

		BufferedReader reader = new BufferedReader(new FileReader(input));
		String line = null;
		while ((line = reader.readLine()) != null) {
			String [] HierarchicalClusterIDs = line.split(",");
			int leftID = Integer.parseInt(HierarchicalClusterIDs[0]);
			int rightID = Integer.parseInt(HierarchicalClusterIDs[1]);
			HierarchicalCluster leftC = graph.get(leftID);
			HierarchicalCluster rightC = graph.get(rightID);
			HierarchicalCluster newC = new HierarchicalCluster(leftC, rightC, HierarchicalClusterID++);
			leftC.setParent(newC);
			rightC.setParent(newC);
			graph.remove(leftID);
			graph.remove(rightID);
			graph.put(leftID, newC);
			HierarchicalClusters.add(newC);
		}
		reader.close();

		if (graph.size() != 1) {
			System.err.println("WARNING: incomplete HierarchicalClustering: graph.size = " + graph.size());
		}

		HierarchicalCluster root = graph.entrySet().iterator().next().getValue();

		return new HierarchicalClusterGraph(pointVectors, pointIndex, HierarchicalClusters, root);
	}

	public static HierarchicalClusterGraph parseDump(File outputFile) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(outputFile));
		String line = null;
		Map<Integer, HierarchicalCluster> graph = new HashMap<Integer, HierarchicalCluster>();
		List<HierarchicalCluster> HierarchicalClusters = new ArrayList<HierarchicalCluster>();
		Set<Integer> HierarchicalClusterIDs = new HashSet<Integer>();
		while ((line = reader.readLine()) != null) {
			HierarchicalCluster c = HierarchicalCluster.parseString(line);
			HierarchicalClusters.add(c);
			graph.put(c.getID(), c);
			HierarchicalClusterIDs.add(c.getID());
		}
		reader.close();

		for (HierarchicalCluster c: HierarchicalClusters) {
			if (c.getLeft() == null || c.getRight() == null) {
				continue;
			}
			c.setLeft(graph.get(c.getLeft().getID()));
			c.setRight(graph.get(c.getRight().getID()));
			HierarchicalClusterIDs.remove(c.getLeft().getID());
			HierarchicalClusterIDs.remove(c.getRight().getID());
		}
		HierarchicalCluster root = graph.get(HierarchicalClusterIDs.iterator().next());

		return new HierarchicalClusterGraph(null, null, HierarchicalClusters, root);
	}

	public HierarchicalCluster getRoot() {
		return root;
	}

	public List<HierarchicalCluster> getClusters() {
		return HierarchicalClusters;
	}

	// for debugging purpose
	public void setPointVectors(Path pointInput, FileSystem fs) throws IOException {
		SequenceDirectoryReader<Integer, Vector> pointReader = new SequenceDirectoryReader<Integer, Vector>(pointInput, fs, Integer.class, Vector.class);
		pointIndex = new HashMap<Integer, Integer>();
		pointVectors = new ArrayList<Vector>();
		while (pointReader.seekNext()) {
			pointIndex.put(pointVectors.size(), pointReader.key());
			pointVectors.add(pointReader.val());
		}
		pointReader.close();
	}

	public void setDensity(DistanceMeasure measure) throws InterruptedException {
		if (pointVectors.isEmpty()) {
			throw new InterruptedException("pointVectors are not set");
		}
		double[][] distance = calcDistance(measure);
		for (HierarchicalCluster c: HierarchicalClusters) {
			c.setDensity(distance);
		}
	}

	public void setCentroidString(Map<Integer, String> topicStr) {
		for (HierarchicalCluster c: HierarchicalClusters) {
			c.setCentroid(pointVectors, topicStr);
		}
	}

	public void dumpCSV(File outputFile) throws IOException {
		PrintWriter HierarchicalClusterW = JobUtils.getPrintWriter(outputFile);
		HierarchicalClusterW.println("#HierarchicalClusterID,size,density,leftCID,rightCID,centroid...,pointIDs...");
		for (HierarchicalCluster c: HierarchicalClusters) {
			HierarchicalClusterW.println(c.toString());
		}
		HierarchicalClusterW.close();
	}

	protected double [][] calcDistance(final DistanceMeasure measure) throws InterruptedException {
		final double [][] distance = new double[pointVectors.size()][];
		for (int i = 0; i < pointVectors.size(); i++) {
			distance[i] = new double[i + 1];
		}

		final int numCore = Runtime.getRuntime().availableProcessors();
		Thread [] t = new Thread[numCore];
		for (int n = 0; n < numCore; n++) {
			final int N = n;
			t[n] = new Thread() {
				public void run() { // no races!
					for (int i = N; i < pointVectors.size() - 1; i += numCore)  {
						for (int j = 0; j <= i; j++) {
							distance[i][j] = measure.distance(pointVectors.get(i + 1), pointVectors.get(j));
						}
					}
				}
			};
			t[n].start();
		}
		for (Thread a: t) {
			a.join();
		}
		return distance;
	}
}
