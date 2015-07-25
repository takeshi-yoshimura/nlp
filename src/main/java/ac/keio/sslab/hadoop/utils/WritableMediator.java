package ac.keio.sslab.hadoop.utils;

import java.lang.reflect.ParameterizedType;

import org.apache.hadoop.io.Writable;

public abstract class WritableMediator<T, W extends Writable> {
	public abstract void set(T t);
	public abstract T get();

	public W writable;
	public Class<W> writableClass;

	@SuppressWarnings("unchecked")
	public WritableMediator() {
		// Reflection Hack!
		writableClass = (Class<W>)((ParameterizedType)getClass().getGenericSuperclass()).getActualTypeArguments()[1];
		try {
			writable = writableClass.newInstance();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
