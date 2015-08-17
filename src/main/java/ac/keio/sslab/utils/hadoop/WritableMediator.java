package ac.keio.sslab.utils.hadoop;

import org.apache.hadoop.io.Writable;

public abstract class WritableMediator<T, W extends Writable> {
	public abstract void set(T t);
	public abstract T get();
	public abstract W newWritable();

	public W writable;
	public Class<?> writableClass;

	public WritableMediator() {
		writable = newWritable();
		this.writableClass = writable.getClass();
	}
}
