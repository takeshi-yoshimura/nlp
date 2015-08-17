package ac.keio.sslab.utils.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import ac.keio.sslab.nlp.JobUtils;

// Do not inherit this class due to generics hacks
// output results are always destroyed unless close() is called
public final class SequenceWriter<K, V> implements AutoCloseable {

	private SequenceFile.Writer writer;
	private Path outputPath;

	private WritableMediator<K, ? extends Writable> keyM;
	private WritableMediator<V, ? extends Writable> valueM;

	public SequenceWriter(Path outputPath, FileSystem fs, boolean forceOverwrite, Class<K> keyClass, Class<V> valueClass) throws IOException {
		this.outputPath = outputPath;

		try {
			WritableMediatorFactory<K> keyFactory = new WritableMediatorFactory<>(keyClass);
			WritableMediatorFactory<V> valueFactory = new WritableMediatorFactory<>(valueClass);
			keyM = keyFactory.getMediator();
			valueM = valueFactory.getMediator();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Instantiation failure");
		}

		SequenceFile.Writer.Option fileOpt = SequenceFile.Writer.file(outputPath);
		SequenceFile.Writer.Option keyWritableClass = SequenceFile.Writer.keyClass(keyM.writable.getClass());
		SequenceFile.Writer.Option valueWritableClass = SequenceFile.Writer.valueClass(valueM.writable.getClass());

		if (fs.exists(outputPath)) {
			if (!JobUtils.promptDeleteDirectory(fs, outputPath, forceOverwrite)) {
				throw new IOException("Revoke writes on " + outputPath);
			}
		}
		fs.mkdirs(outputPath.getParent());

		writer = SequenceFile.createWriter(fs.getConf(), fileOpt, keyWritableClass, valueWritableClass);
	}

	public void append(K key, V value) throws IOException {
		keyM.set(key); valueM.set(value);
		writer.append(keyM.writable, valueM.writable);
	}

	public void append(Writable key, Writable value) throws IOException {
		writer.append(key, value);
	}

	@Override
	public void close() throws IOException {
		if (outputPath != null) {
			writer.close();
			outputPath = null;
		}
	}
}
