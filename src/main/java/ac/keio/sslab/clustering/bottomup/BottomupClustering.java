package ac.keio.sslab.clustering.bottomup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;

// select and run the best algorithm of bottom-up clustering
// try to use as small amounts of memory as possible here because memory may not be able to store all the input data
public class BottomupClustering implements BottomupClusteringListener {

	BottomupClusteringListener clustering;
	Map<Integer, Integer> pointIndex;
	Map<Integer, Vector> newVectors;
	Path input;
	Configuration conf;
	long threasholdNumPoints;
	int merged, merging;

	protected List<Vector> getPoints() throws Exception {
		List<Vector> ret = new ArrayList<Vector>();
		pointIndex = new HashMap<Integer, Integer>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, conf, Integer.class, Vector.class);
		while (reader.seekNext()) {
			pointIndex.put(ret.size(), reader.key());
			ret.add(reader.val());
		}
		reader.close();
		return ret;
	}

	public BottomupClustering(Path input, Configuration conf, String measureName, long threasholdMemorySize) throws Exception {
		this.input = input;
		this.conf = conf;

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
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, conf, Integer.class, Vector.class);
		reader.seekNext();
		numD = reader.val().size();
		numP++;
		while (reader.seekNext()) {
			numP++;
		}
		reader.close();

		if (numP * numP * 4 < threasholdMemorySize) { // e.g., 40K * 40K * 4 = 1600M * 4 = 6.4G
			clustering = new InMemoryFastBottomupClustering(getPoints(), measure);
			threasholdNumPoints = 0;
		} else if (8 * numD * numP < threasholdMemorySize) {
			clustering = new InMemorySlowBottomupClustering(getPoints(), measure);
			threasholdNumPoints = (long) Math.sqrt(threasholdMemorySize / 4);
		} else {
			throw new Exception("Too many points (" + numP + ") or large dimensions (" + numD + "). Implement InDiskBottomupClustering");
		}

		newVectors = new HashMap<Integer, Vector>();
	}

	@Override
	public boolean next() {
		if (!clustering.next()) {
			return false;
		}
		int orig_merged = clustering.mergedPointId();
		int orig_merging = clustering.mergingPointId();
		merged = pointIndex.get(orig_merged);
		merging = pointIndex.get(orig_merging);

		pointIndex.remove(orig_merged);
		newVectors.put(merging, clustering.newPointVector());
		if (newVectors.containsKey(orig_merged)) {
			newVectors.remove(orig_merged);
		}

		//TODO: define another threashold when memory cannot store all the points (after implement InDiskBottomupClustering).
		// In that case, need to consider newVectors may cause out-of-memory.
		if (pointIndex.size() < threasholdNumPoints) {
			try {
				List<Vector> newPoints = new ArrayList<Vector>();
				Map<Integer, Integer> newPointIndex = new HashMap<Integer, Integer>();
				SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(input, conf, Integer.class, Vector.class);
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
				clustering = new InMemoryFastBottomupClustering(newPoints, clustering.getDistanceMeasure());
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

	@Override
	public int mergedPointId() {
		return merged;
	}

	@Override
	public int mergingPointId() {
		return merging;
	}

	@Override
	public DistanceMeasure getDistanceMeasure() {
		return clustering.getDistanceMeasure();
	}

	@Override
	public Vector newPointVector() {
		return clustering.newPointVector();
	}
}
