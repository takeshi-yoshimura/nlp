package ac.keio.sslab.clustering.bottomup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;

public class MergingMergedDumper {

	public class cluster {
		public cluster leftC, rightC;
		public int size;
		public int ID;
		public Vector centroid;
		public List<Integer> topics;
		public List<Double> topicProbs;

		public cluster(cluster leftC, cluster rightC, int ID) {
			this.leftC = leftC;
			this.rightC = rightC;
			this.size = leftC.size + rightC.size;
			this.centroid = leftC.centroid.plus(rightC.centroid).divide(2);
			this.ID = ID;
		}

		public cluster(Vector v, int ID) {
			this.leftC = this.rightC = null;
			this.size = 1;
			this.centroid = v;
			this.ID = ID;
		}
	}

	File input;
	Path pointInput;
	FileSystem fs, fs2;
	cluster root;
	List<cluster> clusters;

	// topicStr can be null
	public MergingMergedDumper(File input, FileSystem fs, Path pointInput, FileSystem fs2) throws IOException {
		this.input = input;
		this.fs = fs;
		this.pointInput = pointInput;
		this.fs2 = fs2;
		buildGraph();
	}

	protected void buildGraph() throws IOException {
		Map<Integer, cluster> graph = new HashMap<Integer, cluster>();
		clusters = new ArrayList<cluster>();
		SequenceDirectoryReader<Integer, Vector> pointReader = new SequenceDirectoryReader<Integer, Vector>(pointInput, fs2, Integer.class, Vector.class);
		int clusterID = 0;
		while (pointReader.seekNext()) {
			cluster c = new cluster(pointReader.val(), clusterID++);
			graph.put(pointReader.key(), c);
			clusters.add(c);
		}
		pointReader.close();

		BufferedReader reader = new BufferedReader(new FileReader(input));
		String line = null;
		clusters = new ArrayList<cluster>();
		while ((line = reader.readLine()) != null) {
			String [] clusterIDs = line.split(",");
			int leftID = Integer.parseInt(clusterIDs[0]);
			int rightID = Integer.parseInt(clusterIDs[1]);
			cluster leftC = graph.get(leftID);
			cluster rightC = graph.get(rightID);
			cluster newC = new cluster(leftC, rightC, clusterID++);
			graph.remove(leftID);
			graph.remove(rightID);
			graph.put(leftID, newC);
			clusters.add(newC);
		}
		reader.close();

		if (graph.size() != 1) {
			System.err.println("WARNING: incomplete clustering: graph.size = " + graph.size());
		}
		root = graph.entrySet().iterator().next().getValue();
	}

	public cluster getRoot() {
		return root;
	}

	public void dumpCSV(File outputDir, Map<Integer, String> topicStr) throws IOException {
		PrintWriter graphW = JobUtils.getPrintWriter(new File(outputDir, "graphEdge.csv"));
		graphW.println("#parentClusterID,childClusterID");
		PrintWriter clusterW = JobUtils.getPrintWriter(new File(outputDir, "clusters.csv"));
		graphW.println("#clusterID,size,centroid");
		for (cluster c: clusters) {
			clusterW.print(c.ID + "," + c.size);
			int i = 0;
			for (Entry<Integer, Double> e: JobUtils.getTopElements(c.centroid, 3)) {
				if (++i > 3) {
					break;
				}
				clusterW.print("," + (topicStr == null ? e.getKey(): topicStr.get(e.getKey())) + ":" + String.format("%1$3f", e.getValue()));
			}
			clusterW.println();
			if (c.leftC != null && c.rightC != null) {
				graphW.println(c.ID + "," + c.leftC.ID);
				graphW.println(c.ID + "," + c.rightC.ID);
			}
		}
		graphW.close();
		clusterW.close();
	}

	public void dumpDotFromRoot(File outputDir, Map<Integer, String> topicStr, int numHierarchy) throws IOException {
		dumpDot(outputDir, topicStr, root.ID, numHierarchy);
	}

	public void dumpDot(File outputDir, Map<Integer, String> topicStr, int startID, int numHierarchy) throws IOException {
		String dendName = "dend_" + startID + "_" + numHierarchy;
		PrintWriter writer = JobUtils.getPrintWriter(new File(outputDir, dendName + ".dot"));
		writer.println("digraph " + dendName + " {");
		cluster current = null;
		for (cluster c: clusters) {
			if (c.ID == startID) {
				current = c;
			}
		}
		dumpDotTraverse(writer, topicStr, current, numHierarchy);
		writer.println("}");
		writer.close();
	}

	private void dumpDotTraverse(PrintWriter writer, Map<Integer, String> topicStr, cluster current, int numHierarchy) {
		if (current == null || numHierarchy <= 0) {
			return;
		}

		writer.print("\tC" + current.ID + " [shape = record, label=\"{{C" + current.ID + " | N(p) = " + current.size + "}");
		int i = 0;
		for (Entry<Integer, Double> e: JobUtils.getTopElements(current.centroid, 3)) {
			if (++i > 3) {
				break;
			}
			writer.print("|" + (topicStr == null ? e.getKey(): topicStr.get(e.getKey())) + ":" + String.format("%1$3f", e.getValue()));
		}
		writer.println("}\"];");

		if (current.leftC != null) {
			writer.println("\tC" + current.ID + " -> " + "C" + current.leftC.ID + ";");
		}
		if (current.rightC != null) {
			writer.println("\tC" + current.ID + " -> " + "C" + current.rightC.ID + ";");
		}
		writer.println();

		dumpDotTraverse(writer, topicStr, current.leftC, numHierarchy - 1);
		dumpDotTraverse(writer, topicStr, current.rightC, numHierarchy - 1);
	}
}
