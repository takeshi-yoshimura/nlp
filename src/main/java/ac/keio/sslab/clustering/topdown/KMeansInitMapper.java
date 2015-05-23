package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class KMeansInitMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, ScoredVectorWritable> {

	int numClusterDivision;
	int defaultNumPointsForCentroidCalc;
	Multimap<Integer, Vector> points;
	Random random;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();

		numClusterDivision = KMeansInitDriver.SideData
				.getNumClusterDivision(conf);
		defaultNumPointsForCentroidCalc = KMeansInitDriver.SideData
				.getDefaultNumPointsForCentroidCalc(conf);
		points = HashMultimap.create();
		random = RandomUtils.getRandom();
	}

	@Override
	public void map(IntWritable key, VectorWritable value, Context context) {
		/*
		 * input key = (parent "cluster" ID), input value = (point Vector) in
		 * this mapper's cleanup, send <child cluster ID, random selected point
		 * Vectors> to the reducer
		 */
		points.put(key.get(), value.get());
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		IntWritable outKey = new IntWritable();
		ScoredVectorWritable outValue = new ScoredVectorWritable();
		Set<Integer> chosenIntSet = new HashSet<Integer>();

		for (int oldClusterID : points.keySet()) {
			/*
			 * choose 10 random points and calculate a centroid for generating
			 * kmeans initial points. the mapper does the choice and the reducer
			 * does the calculation. but we have to handle simultaneously
			 * unrelated clusters in order to avoid large performance loss
			 */
			Object [] vectors = points.get(oldClusterID).toArray();

			if (numClusterDivision <= vectors.length) {
				int numPointsForCentroidCalc = defaultNumPointsForCentroidCalc;
				if (numClusterDivision * numPointsForCentroidCalc > vectors.length) {
					numPointsForCentroidCalc = vectors.length
							/ numClusterDivision;
				}

				while (chosenIntSet.size() < numClusterDivision
						* numPointsForCentroidCalc) {
					chosenIntSet.add(random.nextInt(vectors.length));
				}

				Iterator<Integer> chosenIntIterator = chosenIntSet
						.iterator();
				for (int k = 0; k < numClusterDivision; k++) {
					/*
					 * Name of cluster IDs: in top down clustering, all the
					 * child clusters have a parent cluster. by using
					 * parent-child expression, (child ID) = (parent ID) *
					 * numClusterDivision + [0 ~ (numClusterDivision - 1)]
					 */
					int newClusterID = oldClusterID * numClusterDivision + k;
					/*
					 * maybe points are not uniformly distributed, so output
					 * with score (= number of processed points) and use it as
					 * weight in the reduce side. otherwise we cannot choice
					 * totally uniform, random inputs
					 */
					outKey.set(newClusterID);
					outValue.setScore(vectors.length);
					for (int t = 0; t < numPointsForCentroidCalc; t++) {
						outValue.setVector((Vector)vectors[chosenIntIterator.next()]);
						context.write(outKey, outValue);
					}
				}
			} else {
				/*
				 * maybe orphan points inside this mapper (otherwise reach the
				 * smallest cluster), so output all with a new cluster ID.
				 * Orphan points could be uniformly distributed among mappers,
				 * but even in that case, we can reach a small size (at most (#
				 * of mappers) * numClusterDivision) cluster
				 */
				int newClusterID = oldClusterID * numClusterDivision;
				outKey.set(newClusterID);
				outValue.setScore(vectors.length);
				for (Object vector: vectors) {
					outValue.setVector((Vector)vector);
					context.write(outKey, outValue);
				}
			}
			chosenIntSet.clear();
		}
	}
}
