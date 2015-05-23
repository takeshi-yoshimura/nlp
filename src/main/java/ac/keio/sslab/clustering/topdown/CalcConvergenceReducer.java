package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.mahout.clustering.iterator.ClusterWritable;

public class CalcConvergenceReducer extends
		Reducer<IntWritable, ClusterWritable, IntWritable, ClusterWritable> {
	ClusterWritable outValue;
	List<TopDownKMeansCluster> clusters;
	float convergenceDelta;
	CalcConvergenceDriver.RSSSorter sorter;
	MultipleOutputs<IntWritable, ClusterWritable> output;

	@Override
	public void setup(Context context) {
		outValue = new ClusterWritable();
		convergenceDelta = CalcConvergenceDriver.SideData
				.getConvergenceDelta(context.getConfiguration());
		clusters = new ArrayList<TopDownKMeansCluster>();
		sorter = new CalcConvergenceDriver.RSSSorter();
		output = new MultipleOutputs<IntWritable, ClusterWritable>(context);
	}

	@Override
	public void reduce(IntWritable key, Iterable<ClusterWritable> values,
			Context context) throws IOException, InterruptedException {
		for (ClusterWritable value : values) {
			clusters.add((TopDownKMeansCluster) value.getValue());
		}
		Collections.sort(clusters, sorter);
		TopDownKMeansCluster cluster = clusters.get(0);

		if (clusters.size() == 3) {
			cluster.setConverged(CalcConvergenceDriver.isConverged(clusters, convergenceDelta));
		} else if (clusters.size() != 1) {
			throw new IllegalArgumentException("clusters.size != 1, 3");
		}

		outValue.setValue(cluster);
		if (cluster.isConverged()) {
			output.write(key,  outValue, "converged/clusters");
		} else {
			context.getCounter(CalcConvergenceDriver.ConvergenceCounter.unconverged).increment(1);
			output.write(key, outValue, "unconverged/clusters");
		}
		
		clusters.clear();
	}
	
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		output.close();
	}
}
