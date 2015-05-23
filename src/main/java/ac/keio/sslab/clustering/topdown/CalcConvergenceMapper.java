package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.iterator.ClusterWritable;

public class CalcConvergenceMapper extends
		Mapper<IntWritable, ClusterWritable, IntWritable, ClusterWritable> {

	Map<Integer, List<TopDownKMeansCluster>> map;
	float convergenceDelta;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		convergenceDelta = CalcConvergenceDriver.SideData
				.getConvergenceDelta(conf);
		map = new HashMap<Integer, List<TopDownKMeansCluster>>();
	}

	@Override
	public void map(IntWritable key, ClusterWritable value, Context context) {
		int k = key.get();
		List<TopDownKMeansCluster> list;
		if (map.containsKey(k)) {
			list = map.get(k);
		} else {
			list = new ArrayList<TopDownKMeansCluster>();
			map.put(k, list);
		}
		list.add((TopDownKMeansCluster)value.getValue());
	}

	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		IntWritable outKey = new IntWritable();
		ClusterWritable outValue = new ClusterWritable();
		CalcConvergenceDriver.RSSSorter sorter = new CalcConvergenceDriver.RSSSorter();
		
		for (int key : map.keySet()) {
			List<TopDownKMeansCluster> clusters = map.get(key);
			Collections.sort(clusters, sorter);
			
			if (clusters.size() == 3) {
				//reduce key-values sent to the reducer
				TopDownKMeansCluster cluster = clusters.get(0);
				cluster.setConverged(CalcConvergenceDriver.isConverged(clusters, convergenceDelta));
				outKey.set(key);
				outValue.setValue(cluster);
				context.write(outKey, outValue);
			} else if (clusters.size() < 3) {
				//maybe the same key's inputs are distributed to other mappers
				//retry the calculation at the reducer
				for (TopDownKMeansCluster cluster : clusters) {
					outKey.set(key);
					outValue.setValue(cluster);
					context.write(outKey, outValue);
				}
			} else {
				throw new IllegalArgumentException("clusters.size > 3");
			}
		}
	}
}
