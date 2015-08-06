package ac.keio.sslab.clustering.bottomup;

import java.io.File;
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

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.nlp.JobUtils;

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

	Path input, pointInput;
	FileSystem fs, fs2;
	cluster root;
	List<cluster> clusters;

	// topicStr can be null
	public MergingMergedDumper(Path input, FileSystem fs, Path pointInput, FileSystem fs2) throws IOException {
		this.input = input;
		this.fs = fs;
		this.pointInput = pointInput;
		this.fs2 = fs2;
		buildGraph();
	}

	protected void buildGraph() throws IOException {
		Map<Integer, cluster> graph = new HashMap<Integer, cluster>();
		SequenceDirectoryReader<Integer, Vector> pointReader = new SequenceDirectoryReader<Integer, Vector>(pointInput, fs2, Integer.class, Vector.class);
		int clusterID = 0;
		while (pointReader.seekNext()) {
			cluster c = new cluster(pointReader.val(), clusterID++);
			graph.put(pointReader.key(), c);
			clusters.add(c);
		}
		pointReader.close();

		SequenceDirectoryReader<Integer, Integer> reader = new SequenceDirectoryReader<Integer, Integer>(input, fs, Integer.class, Integer.class);
		clusters = new ArrayList<cluster>();
		while (reader.seekNext()) {
			cluster leftC = graph.get(reader.key());
			cluster rightC = graph.get(reader.val());
			cluster newC = new cluster(leftC, rightC, clusterID++);
			graph.remove(reader.key());
			graph.remove(reader.val());
			graph.put(reader.key(), newC);
			clusters.add(newC);
		}
		reader.close();

		if (graph.size() != 1) {
			System.err.println("WARNING: incomplete clustering: clusters.size = " + clusters.size());
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
			for (Entry<Integer, Double> e: JobUtils.getTopElements(c.centroid, 3)) {
				clusterW.print("," + (topicStr == null ? e.getKey(): topicStr.get(e.getKey())) + ":" + String.format("%1$3", e.getValue()));
			}
			clusterW.println();
			graphW.println(c.ID + "," + c.leftC.ID);
			graphW.println(c.ID + "," + c.rightC.ID);
		}
		graphW.close();
		clusterW.close();
	}
}
