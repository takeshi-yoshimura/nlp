package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.iterator.ClusterWritable;

public class KMeansReducer extends
		Reducer<IntWritable, ClusterWritable, IntWritable, ClusterWritable> {

	ClusterWritable outValue;
	IntWritable outKey;
	Map<Integer, TopDownKMeansCluster> newClusters;

	@Override
	public void setup(Context context) {
		outKey = new IntWritable();
		outValue = new ClusterWritable();
		newClusters = new HashMap<Integer, TopDownKMeansCluster>();
	}

	@Override
	public void reduce(IntWritable key, Iterable<ClusterWritable> values,
			Context context) throws IOException, InterruptedException {
		/*
		 * input key: old cluster ID, output values: <new cluster ID, cluster>
		 */

		// first, gather info for each new cluster ID and translate dummy
		// cluster into RSS
		double RSS = 0;
		for (ClusterWritable value : values) {
			int newClusterID = value.getValue().getId();
			if (newClusterID < 0) { //dummy ID
				TopDownKMeansCluster cluster = (TopDownKMeansCluster) value
						.getValue();
				RSS += cluster.getRSSk();
			} else if (newClusters.containsKey(newClusterID)) {
				// every calculation is done inside cluster objects
				newClusters.get(newClusterID).observe(value.getValue());
			} else {
				newClusters.put(newClusterID,
						(TopDownKMeansCluster) value.getValue());
			}
		}

		// finally, output all the results
		for (int newClusterID : newClusters.keySet()) {
			TopDownKMeansCluster newCluster = newClusters.get(newClusterID);
			newCluster.computeParameters();
			if (RSS != 0) {
				// convergence confirmation is delayed (note that the mapper
				// uses prior prior clusters for the calculation)
				// but it's OK because clustering quality always gets better for
				// each M/R iteration
				newCluster.setRSSk(RSS);
			}
			outKey.set(newClusterID);
			outValue.setValue(newCluster);
			context.write(outKey, outValue);
		}

		// re-use the next reduce call to save memory
		newClusters.clear();
	}
}
