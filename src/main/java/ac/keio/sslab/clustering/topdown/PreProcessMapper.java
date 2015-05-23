package ac.keio.sslab.clustering.topdown;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

public class PreProcessMapper extends
		Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {
	
	IntWritable outKey = new IntWritable(1);
	
	@Override
	public void map(IntWritable key, VectorWritable value, Context context)
			throws IOException, InterruptedException {
		// For very first of top down kmeans (no reducer)
		context.write(outKey, value);
	}
}
