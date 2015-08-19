package ac.keio.sslab.clustering.bottomup;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;

public class BottomupClustering {

	CachedBottomupClustering clustering;
	Map<Integer, Integer> pointIndex;
	Path input;
	FileSystem fs;

	public BottomupClustering(Path input, FileSystem fs, DistanceMeasure measure) throws Exception {
		this.input = input;
		List<Vector> points = new ArrayList<Vector>();
		pointIndex = new HashMap<Integer, Integer>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, fs, Integer.class, Vector.class);
		while (reader.seekNext()) {
			pointIndex.put(points.size(), reader.key());
			points.add(reader.val());
		}
		reader.close();
		clustering = new CachedBottomupClustering(points, measure);
	}

	public void run(File output, boolean doForceWrite) throws Exception {
		PrintWriter writer = JobUtils.getPrintWriter(output);
		int i = 0;
		int [] nextPair = null;
		while((nextPair = clustering.popMostSimilarClusterPair()) != null) {
			int merging = pointIndex.get(nextPair[0]);
			int merged = pointIndex.get(nextPair[1]);
			System.out.println("Iteration #" + i++ + ": " + merging + "," + merged);
			writer.println(merging + "," + merged);
		}
		writer.close();
	}
}
