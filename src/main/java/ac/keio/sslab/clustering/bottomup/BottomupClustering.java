package ac.keio.sslab.clustering.bottomup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import ac.keio.sslab.hadoop.utils.SequenceWriter;

// select and run the best algorithm of bottom-up clustering
// try to use as small amounts of memory as possible here because memory may not be able to store all the input data
public class BottomupClustering {

	BottomupClusteringAlgorithm clustering;
	Map<Integer, Integer> pointIndex;
	Map<Integer, Vector> newVectors;
	Path input;
	FileSystem fs;
	long threasholdNumPoints;
	int merged, merging;

	boolean threaded;

	protected List<Vector> getPoints() throws Exception {
		List<Vector> ret = new ArrayList<Vector>();
		pointIndex = new HashMap<Integer, Integer>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, fs, Integer.class, Vector.class);
		while (reader.seekNext()) {
			pointIndex.put(ret.size(), reader.key());
			ret.add(reader.val());
		}
		reader.close();
		return ret;
	}

	public BottomupClustering(Path input, FileSystem fs, String measureName, long threasholdMemorySize, boolean threaded) throws Exception {
		this.input = input;
		this.threaded = threaded;

		DistanceMeasure measure;
		if (measureName.toLowerCase().equals("cosine")) {
			measure = new CosineDistanceMeasure();
		} else if (measureName.toLowerCase().equals("euclidean")) {
			measure = new SquaredEuclideanDistanceMeasure();
		} else {
			throw new Exception("Specify Cosine or Euclidean ");
		}

		long numP = 0;
		int numD;
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, fs, Integer.class, Vector.class);
		reader.seekNext();
		numD = reader.val().size();
		numP++;
		while (reader.seekNext()) {
			numP++;
		}
		reader.close();

		if (numP * numP * 4 < threasholdMemorySize) { // e.g., 40K * 40K * 4 = 1600M * 4 = 6.4G
			clustering = threaded ? new ThreadedFastBottomupClustering(getPoints(), measure): new FastBottomupClustering(getPoints(), measure);
			threasholdNumPoints = 0;
		} else if (8 * numD * numP < threasholdMemorySize) {
			clustering = threaded ? new ThreadedBasicBottomupClustering(getPoints(), measure): new BasicBottomupClustering(getPoints(), measure);
			threasholdNumPoints = (long) Math.sqrt(threasholdMemorySize / 4);
		} else {
			throw new Exception("Too many points (" + numP + ") or large dimensions (" + numD + "). Implement InDiskBottomupClustering");
		}

		newVectors = new HashMap<Integer, Vector>();
	}

	public boolean next() {
		if (!clustering.next()) {
			return false;
		}
		Vector newPointVector = clustering.update();

		int orig_merged = clustering.mergedPointId();
		int orig_merging = clustering.mergingPointId();
		merged = pointIndex.get(orig_merged);
		merging = pointIndex.get(orig_merging);

		pointIndex.remove(orig_merged);
		newVectors.put(merging, newPointVector);
		if (newVectors.containsKey(orig_merged)) {
			newVectors.remove(orig_merged);
		}

		//TODO: define another threashold when memory cannot store all the points (after implement InDiskBottomupClustering).
		// In that case, need to consider newVectors may cause out-of-memory.
		if (pointIndex.size() < threasholdNumPoints) {
			try {
				List<Vector> newPoints = new ArrayList<Vector>();
				Map<Integer, Integer> newPointIndex = new HashMap<Integer, Integer>();
				SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, fs, Integer.class, Vector.class);
				Collection<Integer> cached = pointIndex.values();
				while (reader.seekNext()) {
					if (newVectors.containsKey(reader.key())) {
						newPointIndex.put(newPoints.size(), reader.key());
						newPoints.add(newVectors.get(reader.key()));
					} else if (cached.contains(reader.key())) {
						newPointIndex.put(newPoints.size(), reader.key());
						newPoints.add(reader.val());
					}
				}
				reader.close();
				clustering = threaded ? new ThreadedFastBottomupClustering(getPoints(), clustering.getDistanceMeasure()): new FastBottomupClustering(getPoints(), clustering.getDistanceMeasure());
				pointIndex = newPointIndex;
				newVectors.clear();
				threasholdNumPoints = 0;
				System.out.println("Change the algorithm of clustering");
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Something went wrong when changing clustering algorithm");
				clustering = null;
				pointIndex = null;
			}
		}

		return true;
	}

	public void run(Path output, FileSystem fs, boolean doForceWrite) throws IOException {
		SequenceWriter<Integer, Integer> writer = null;
		try {
			writer = new SequenceWriter<Integer, Integer>(output, fs, doForceWrite, Integer.class, Integer.class);
			while(next()) {
				writer.append(clustering.mergingPointId(), clustering.mergedPointId());
			}
			writer.close();
		} catch (IOException e) {
			if (writer != null) {
				writer.close(); // ensure persistent intermediate data
			}
			throw e;
		}
	}

	public void restore(Path output, FileSystem fs) throws IOException {
		Map<Integer, Integer> revPointIndex = new HashMap<Integer, Integer>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, fs, Integer.class, Vector.class);
		while (reader.seekNext()) {
			revPointIndex.put(reader.key(), revPointIndex.size());
		}
		reader.close();

		SequenceDirectoryReader<Integer, Integer> reader2 = new SequenceDirectoryReader<Integer, Integer>(output, fs.getConf(), Integer.class, Integer.class);
		while (reader2.seekNext()) {
			int merging = reader2.key();
			int merged = reader2.val();

			int orig_merged = revPointIndex.get(merged);
			pointIndex.remove(orig_merged);

			newVectors.put(merging, clustering.update());
			if (newVectors.containsKey(orig_merged)) {
				newVectors.remove(orig_merged);
			}
		}
		reader2.close();
	}
}
