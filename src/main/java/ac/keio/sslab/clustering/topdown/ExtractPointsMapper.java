package ac.keio.sslab.clustering.topdown;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

public class ExtractPointsMapper extends
	Mapper<IntWritable, IntWritable, IntWritable, VectorWritable> {

	int extractID;
	VectorWritable outValue = new VectorWritable();
	
	@Override
	public void setup(Context context) {
		extractID = Integer.parseInt(context.getConfiguration().get("jp.ac.keio.ics.sslab.yoshimura.ExtractPointsDriver.extractID"));
	}

	@Override
	public void map(IntWritable key, IntWritable value, Context context) 
			throws IOException, InterruptedException {
		if (value.get() == extractID) {
			DenseVector vec = new DenseVector(1);
			vec.set(0, (double)value.get());
			outValue.set(vec);
			context.write(key, outValue);
		}
	}
}
