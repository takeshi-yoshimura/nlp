package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Vector;

public class KMeansInitReducer
		extends
		Reducer<IntWritable, ScoredVectorWritable, IntWritable, ClusterWritable> {

	int numClusterDivision;
	int defaultNumPointsForCentroidCalc;
	Random random;
	ClusterWritable outValue;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		numClusterDivision = KMeansInitDriver.SideData
				.getNumClusterDivision(conf);
		defaultNumPointsForCentroidCalc = KMeansInitDriver.SideData
				.getDefaultNumPointsForCentroidCalc(conf);
		random = RandomUtils.getRandom();
		outValue = new ClusterWritable();
	}

	@Override
	public void reduce(IntWritable key, Iterable<ScoredVectorWritable> values,
			Context context) throws IOException, InterruptedException {
		/*
		 * input key = (new (child) cluster ID), input values = (point Vectors
		 * with score gathered from all the mapper) this reducer does final
		 * calculation of <new cluster ID, Cluster> The Cluster is calculated by
		 * using vectors randomly selected from all the mapper's outputs For
		 * fair randomness, use score as weight in this reducer
		 */
		int sumScore = 0;
		List<Pair<Integer, Vector>> candidates = new ArrayList<Pair<Integer, Vector>>();
		for (ScoredVectorWritable value : values) {
			Vector candidate = value.getVector();
			int score = value.getScore();
			candidates.add(new Pair<Integer, Vector>(score, candidate));
			sumScore += score;
		}
		
		Set<Integer> chosenIndexSet = new HashSet<Integer>();
		TopDownKMeansCluster cluster = new TopDownKMeansCluster(key.get());
		if (candidates.size() > defaultNumPointsForCentroidCalc * numClusterDivision) {
			// use the centroid among randomly selected points as initial centroid
			// weighted lottery selection
			while (chosenIndexSet.size() != defaultNumPointsForCentroidCalc && chosenIndexSet.size() < candidates.size()) {
				int tmpSumScore = (int) (sumScore * random.nextDouble());
				for (int c = 0; c < candidates.size(); c++) {
					// in order to avoid selecting the same point more than once
					if (chosenIndexSet.contains(c))
						continue;
					tmpSumScore -= candidates.get(c).getFirst();
					if (tmpSumScore < 0) {
						chosenIndexSet.add(c);
						// everything is done inside cluster class.
						cluster.observe(candidates.get(c).getSecond());
						break;
					}
				}
			}
			// probably output the same centroid as other clusters
			// in that case next kmeans iteration will be useless
			// but if candidates.size is large, the problem rarely occurs
		} else {
			// if clustering is almost finished, number of inputs get smaller.
			// so use a point as the centroid
			// it is necessary for avoiding the same centroid as other clusters
			cluster.observe(candidates.get(0).getSecond());
		}
		cluster.computeParameters(); // calculate the centroid, etc.
		outValue.setValue(cluster);
		context.write(key, outValue);
	}
}
