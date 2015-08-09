package ac.keio.sslab.clustering.bottomup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;
import ac.keio.sslab.nlp.NLPConf;

// select and run the best algorithm of bottom-up clustering
// try to use as small amounts of memory as possible here because memory may not be able to store all the input data
public class BottomupClustering {

	CachedBottomupClustering clustering;
	Map<Integer, Integer> pointIndex;
	Path input;
	FileSystem fs;

	public BottomupClustering(Path input, FileSystem fs, String measureName, long memoryCapacity) throws Exception {
		this.input = input;

		DistanceMeasure measure;
		if (measureName.toLowerCase().equals("cosine")) {
			measure = new CosineDistanceMeasure();
		} else if (measureName.toLowerCase().equals("euclidean")) {
			measure = new SquaredEuclideanDistanceMeasure();
		} else {
			throw new Exception("Specify Cosine or Euclidean ");
		}

		List<Vector> points = new ArrayList<Vector>();
		pointIndex = new HashMap<Integer, Integer>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, fs, Integer.class, Vector.class);
		int numP = 0, numD = 0;
		while (reader.seekNext()) {
			pointIndex.put(points.size(), reader.key());
			points.add(reader.val());
			if (numP == 0) {
				numD = reader.val().size();
			}
			numP++;
		}
		reader.close();
		clustering = new CachedBottomupClustering(points, measure, memoryCapacity - numD * numP * Double.SIZE / 8);
	}

	public void run(Path output, FileSystem fs, boolean doForceWrite) throws IOException {
		SequenceSwapWriter<Integer, Integer> writer = new SequenceSwapWriter<Integer, Integer>(output, NLPConf.getInstance().tmpPath, fs, doForceWrite, Integer.class, Integer.class);
		while(clustering.next()) {
			clustering.update();
			int merged = pointIndex.get(clustering.mergedClusterId());
			int merging = pointIndex.get(clustering.mergingClusterId());
			writer.append(merging, merged);
		}
		writer.close();
	}
}
