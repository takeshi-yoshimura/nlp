package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.VectorWritable;

public class KMeansMapper extends Mapper<IntWritable, VectorWritable, IntWritable, ClusterWritable> {

	Map<Integer, TopDownKMeansCluster> oldClusters;
	Map<Integer, TopDownKMeansCluster> oldoldClusters;
	Map<Integer, Map<Integer, TopDownKMeansCluster>> newClusters;
	int numClusterDivision;
	DistanceMeasure measure;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		try {
			// read prior outputs via DistributedCache (in a local file system) for
			// calculating new cluster centroids
			oldClusters = KMeansDriver.SideData.getOldClusterMap(conf);

			// read prior prior outputs via DistributedCache (in a local file
			// system) for calculating the clustering convergence
			oldoldClusters = KMeansDriver.SideData.getOldOldClusterMap(conf);
		} catch (URISyntaxException e) {
			throw new InterruptedException("Something bad happend in URI syntax");
		}

		measure = new SquaredEuclideanDistanceMeasure();
		newClusters = new HashMap<Integer, Map<Integer, TopDownKMeansCluster>>(oldClusters.size() * 4 / 3); // optimizing initial capacity
		numClusterDivision = KMeansDriver.SideData.getNumClusterDivision(conf);
	}

	@Override
	public void map(IntWritable key, VectorWritable value, Context context) throws InterruptedException {
		/*
		 * input key: old, prior, parent cluster ID, input value: point vector
		 */

		// first taks of this mapper is to do popular top-down clustering with
		// kmeans but concurrently
		int nearestClusterID = -1;
		double nearestDistance = Double.MAX_VALUE;

		// second task of this mapper is to calculate the convergence of the
		// prior clustering result
		double oldNearestDistance = Double.MAX_VALUE;
		int nearestOldClusterID = -1;

		// simultaneously running both the same depth top-down clustering and
		// convergence calculation for high performance
		for (int i = 0; i < numClusterDivision; i++) {
			// only compare points belonging to the same prior cluster for
			// clustering stability
			int clusterID = key.get() * numClusterDivision + i;

			// For clustering
			TopDownKMeansCluster cluster = oldClusters.get(clusterID);
			if (cluster == null) {
				// clusterID is not contained in HashMap because the cluster is
				// converged or the cluster size is very small
				continue;
			}
			double distance = measure.distance(cluster.getCenter(), value.get());
			if (distance < nearestDistance) {
				nearestClusterID = clusterID;
				nearestDistance = distance;
			}

			// For convergence calculation
			if (oldoldClusters != null) {
				TopDownKMeansCluster oldCluster = oldoldClusters.get(clusterID);
				if (oldCluster == null) {
					//in case of converged cluster
					continue;
				}
				double oldDistance = measure.distance(oldCluster.getCenter(), value.get());
				if (oldDistance < oldNearestDistance) {
					nearestOldClusterID = clusterID;
					oldNearestDistance = oldDistance; 
					// used at the almost end of this function
				}
			}
		}

		// try to reduce output by using in-mapper combiner... so a bit
		// complicated
		Map<Integer, TopDownKMeansCluster> tmpOutputValue;
		if (newClusters.containsKey(key.get())) {
			tmpOutputValue = newClusters.get(key.get());
		} else {
			tmpOutputValue = new HashMap<Integer, TopDownKMeansCluster>();
			newClusters.put(key.get(), tmpOutputValue);
		}
		TopDownKMeansCluster tmpNewCluster;
		if (tmpOutputValue.containsKey(nearestClusterID)) {
			tmpNewCluster = tmpOutputValue.get(nearestClusterID);
		} else {
			tmpNewCluster = new TopDownKMeansCluster(nearestClusterID);
			tmpOutputValue.put(nearestClusterID, tmpNewCluster);
		}
		// this observation result is preserved even at the reducer.
		tmpNewCluster.observe(value.get());

		if (oldNearestDistance == Double.MAX_VALUE) {
			return;
		}

		// add a dummy cluster (ID == -1 + -1 * nearestOldClusterID) to gather convergence 
		// results at the reducer
		int dummyClusterID = -1 + -1 * nearestOldClusterID;
		if (tmpOutputValue.containsKey(dummyClusterID)) {
			TopDownKMeansCluster dummyCluster = tmpOutputValue.get(dummyClusterID);
			dummyCluster.incrementRSSk(value.get());
		} else {
			TopDownKMeansCluster dummyCluster = new TopDownKMeansCluster(oldClusters.get(nearestOldClusterID).getCenter(), dummyClusterID, null);
			dummyCluster.incrementRSSk(value.get());
			tmpOutputValue.put(dummyClusterID, dummyCluster);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		IntWritable outKey = new IntWritable();
		ClusterWritable outValue = new ClusterWritable();
		for (int oldClusterID : newClusters.keySet()) {
			Map<Integer, TopDownKMeansCluster> tmpNewCluster = newClusters.get(oldClusterID);
			for (int clusterID : tmpNewCluster.keySet()) {
				TopDownKMeansCluster cluster = tmpNewCluster.get(clusterID);
				outKey.set(oldClusterID);
				outValue.setValue(cluster);
				context.write(outKey, outValue);
			}
		}
	}
}
