package ac.keio.sslab.utils.hadoop;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class WritableMediatorFactory<T> {

	WritableMediator<T, ? extends Writable> med;

	@SuppressWarnings("unchecked")
	public WritableMediatorFactory(Class<T> type) throws Exception {
		if (type == Integer.class) {
			med = (WritableMediator<T, ? extends Writable>)new IntMediator();
		} else if (type == Double.class) {
			med = (WritableMediator<T, ? extends Writable>)new DoubleMediator();
		} else if (type == Long.class) {
			med = (WritableMediator<T, ? extends Writable>)new LongMediator();
		} else if (type == Vector.class) {
			med = (WritableMediator<T, ? extends Writable>)new VectorMediator();
		} else if (type == Cluster.class) {
			med = (WritableMediator<T, ? extends Writable>)new ClusterMediator();
		} else if (type == Void.class) {
			med = (WritableMediator<T, ? extends Writable>)new VoidMediator();
		} else if (type == String.class) {
			med = (WritableMediator<T, ? extends Writable>)new StringMediator();
		} else {
			throw new InstantiationException("Unsupported class: " + type.getClass());
		}
	}

	public WritableMediator<T, ? extends Writable> getMediator() {
		return med;
	}
}

//T = Integer, W == IntWritable, I expect the compiler removes redundant type conversions like C++ specialized template
class IntMediator extends WritableMediator<Integer, IntWritable> {

	@Override
	public void set(Integer t) {
		writable.set(t);
	}

	@Override
	public Integer get() {
		return writable.get();
	}

	@Override
	public IntWritable newWritable() {
		return new IntWritable();
	}
}

class DoubleMediator extends WritableMediator<Double, DoubleWritable> {

	@Override
	public void set(Double t) {
		writable.set(t);
	}

	@Override
	public Double get() {
		return writable.get();
	}

	@Override
	public DoubleWritable newWritable() {
		return new DoubleWritable();
	}
}

class LongMediator extends WritableMediator<Long, LongWritable> {

	@Override
	public void set(Long t) {
		writable.set(t);
	}

	@Override
	public Long get() {
		return writable.get();
	}

	@Override
	public LongWritable newWritable() {
		return new LongWritable();
	}
}

class VectorMediator extends WritableMediator<Vector, VectorWritable> {

	@Override
	public void set(Vector t) {
		writable.set(t);
	}

	@Override
	public Vector get() {
		return writable.get();
	}

	@Override
	public VectorWritable newWritable() {
		return new VectorWritable();
	}
}

class ClusterMediator extends WritableMediator<Cluster, ClusterWritable> {

	@Override
	public void set(Cluster t) {
		writable.setValue(t);
	}

	@Override
	public Cluster get() {
		return writable.getValue();
	}

	@Override
	public ClusterWritable newWritable() {
		return new ClusterWritable();
	}
}

class VoidMediator extends WritableMediator<Void, NullWritable> {

	@Override
	public void set(Void t) {
	}

	@Override
	public Void get() {
		return null;
	}

	@Override
	public NullWritable newWritable() {
		return NullWritable.get();
	}
}

class StringMediator extends WritableMediator<String, Text> {

	@Override
	public void set(String t) {
		writable.set(t);
	}

	@Override
	public String get() {
		return writable.toString();
	}

	@Override
	public Text newWritable() {
		return new Text();
	}
}
