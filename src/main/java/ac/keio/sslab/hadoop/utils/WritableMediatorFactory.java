package ac.keio.sslab.hadoop.utils;

import java.lang.reflect.ParameterizedType;

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

	public WritableMediatorFactory() throws InstantiationException, IllegalAccessException {
		// Hack!
		@SuppressWarnings("unchecked")
		Class<T> type = (Class<T>)((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[0];

		if (type == Integer.class) {
			med = new IntMediator<T, IntWritable>();
		} else if (type == Double.class) {
			med = new DoubleMediator<T, DoubleWritable>();
		} else if (type == Long.class) {
			med = new LongMediator<T, LongWritable>();
		} else if (type == Vector.class) {
			med = new VectorMediator<T, VectorWritable>();
		} else if (type == Cluster.class) {
			med = new ClusterMediator<T, ClusterWritable>();
		} else if (type == Void.class) {
			med = new VoidMediator<T, NullWritable>();
		} else if (type == String.class) {
			med = new StringMediator<T, Text>();
		} else {
			throw new InstantiationException("Unsupported class: " + type.getName());
		}
	}

	public WritableMediator<T, ? extends Writable> getMediator() {
		return med;
	}
}


//T = Integer, W == IntWritable, I expect the compiler removes redundant type conversions like C++ specialized template
class IntMediator<T, W extends Writable> extends WritableMediator<T, W> {

	@Override
	public void set(T t) {
		((IntWritable)writable).set((Integer)t);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T get() {
		return (T)(Integer)((IntWritable)writable).get();
	}
}

class DoubleMediator<T, W extends Writable> extends WritableMediator<T, W> {

	@Override
	public void set(T t) {
		((DoubleWritable)writable).set((Double)t);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T get() {
		return (T)(Double)((DoubleWritable)writable).get();
	}
}

class LongMediator<T, W extends Writable> extends WritableMediator<T, W> {

	@Override
	public void set(T t) {
		((LongWritable)writable).set((Long)t);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T get() {
		return (T)(Long)((LongWritable)writable).get();
	}
}

class VectorMediator<T, W extends Writable> extends WritableMediator<T, W> {

	@Override
	public void set(T t) {
		((VectorWritable)writable).set((Vector)t);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T get() {
		return (T)((VectorWritable)writable).get();
	}
}

class ClusterMediator<T, W extends Writable> extends WritableMediator<T, W> {

	@Override
	public void set(T t) {
		((ClusterWritable)writable).setValue((Cluster)t);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T get() {
		return (T)((ClusterWritable)writable).getValue();
	}
}

class VoidMediator<T, W extends Writable> extends WritableMediator<T, W> {

	@Override
	public void set(T t) {
	}

	@Override
	public T get() {
		return null;
	}
}

class StringMediator<T, W extends Writable> extends WritableMediator<T, W> {

	@Override
	public void set(T t) {
		((Text)writable).set((String)t);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T get() {
		return (T)((Text)writable).toString();
	}
}
