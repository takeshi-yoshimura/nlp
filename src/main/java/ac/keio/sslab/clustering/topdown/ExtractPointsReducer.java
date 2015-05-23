package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class ExtractPointsReducer extends
		Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	VectorWritable outValue = new VectorWritable();

	@Override
	protected void reduce(IntWritable key, Iterable<VectorWritable> values,
			Context context) throws IOException, InterruptedException {
		Iterator<VectorWritable> i = values.iterator();
		Vector first = i.next().get();
		if (!i.hasNext())
			return;
		Vector second = i.next().get();
		if (first.size() == 1) {
			outValue.set(second);
			context.write(key, outValue);
		} else if (second.size() == 1) {
			outValue.set(first);
			context.write(key, outValue);
		} else {
			int num = 2;
			while (i.hasNext()) {
				i.next();
				num++;
			}
			throw new InterruptedException("Unmatched Vector Input: " 
					+ key.get() + ": " + first.get(0) + " and "
					+ second.get(0) + ", " + num);
		}
	}
}
