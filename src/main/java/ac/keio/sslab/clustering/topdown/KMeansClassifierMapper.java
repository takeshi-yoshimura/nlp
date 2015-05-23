package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.VectorWritable;

public class KMeansClassifierMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	Map<Integer, Cluster> clusters;
	int numClusterDivision;
	IntWritable outKey;
	DistanceMeasure measure;

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();

		// read prior outputs via DistributedCache (in a local file system) for
		// classifying points into clusters
		clusters = KMeansClassifierDriver.SideData.getFinalClusterMap(conf);

		outKey = new IntWritable();
		measure = new SquaredEuclideanDistanceMeasure();
		numClusterDivision = KMeansClassifierDriver.SideData
				.getNumClusterDivision(conf);
	}

	@Override
	public void map(IntWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		/*
		 * input key: old, prior, parent cluster ID, input value: point vector
		 * this mapper outputs <new cluster ID, point vector> outputs are the
		 * classification result (no reducer). Basic algorithm is the same as
		 * TopDownKMeansMapper
		 */
		double nearestDistance = Double.MAX_VALUE;
		int nearestClusterID = -1;

		for (int i = 0; i < numClusterDivision; i++) {
			int clusterID = key.get() * numClusterDivision + i;
			// a cluster may disappear. it often occurs when number of
			// points in the cluster is only one or very small
			if (!clusters.containsKey(clusterID))
				continue;
			double distance = measure.distance(clusters.get(clusterID)
					.getCenter(), value.get());
			if (distance < nearestDistance) {
				nearestClusterID = clusterID;
				nearestDistance = distance;
			}
		}
		// BUG! never allow any points not belonging to any clusters
		if (nearestClusterID == -1)
			throw new InterruptedException("Nearest Cluster was not found at Key ID: " + key.get());

		outKey.set(nearestClusterID);
		context.write(outKey, value);
	}
}
