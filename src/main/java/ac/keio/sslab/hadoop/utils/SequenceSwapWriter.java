package ac.keio.sslab.hadoop.utils;

import java.io.IOException;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import ac.keio.sslab.nlp.JobUtils;

// Do not inherit this class due to generics hacks
// output results are always destroyed unless close() is called
public final class SequenceSwapWriter<K, V> implements AutoCloseable {

	private SequenceFile.Writer tmpWriter, outputLocker;
	private Path tmpPath, outputPath;
	private FileSystem fs;

	private WritableMediator<K, ? extends Writable> keyM;
	private WritableMediator<V, ? extends Writable> valueM;

	public SequenceSwapWriter(Path outputPath, Path tmpDirPath, Configuration conf, boolean forceOverwrite) throws IOException {
		this.outputPath = outputPath;
		fs = FileSystem.get(conf);
		tmpPath = new Path(tmpDirPath, RandomStringUtils.randomAscii(32) + "/" + outputPath.getName());
		while (fs.exists(tmpPath)) {
			tmpPath = new Path(tmpDirPath, RandomStringUtils.randomAscii(32) + "/" + outputPath.getName());
		}
		fs.deleteOnExit(tmpPath.getParent());

		try {
			WritableMediatorFactory<K> keyFactory = new WritableMediatorFactory<K>();
			WritableMediatorFactory<V> valueFactory = new WritableMediatorFactory<V>();
			keyM = keyFactory.getMediator();
			valueM = valueFactory.getMediator();
		} catch (Exception e) {
			throw new IOException("Instantiation failure");
		}

		SequenceFile.Writer.Option fileOpt = SequenceFile.Writer.file(tmpPath);
		SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(keyM.writableClass);
		SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(valueM.writableClass);

		if (fs.exists(outputPath)) {
			if (!JobUtils.promptDeleteDirectory(fs, outputPath, forceOverwrite)) {
				throw new IOException("Revoke writes on " + outputPath);
			}
		}
		fs.mkdirs(outputPath.getParent());

		// Dummy writer for exclusive writes (All the HDFS files are exclusively written)
		SequenceFile.Writer.Option lock = SequenceFile.Writer.file(outputPath);
		outputLocker = SequenceFile.createWriter(conf, lock, keyClass, valueClass);

		tmpWriter = SequenceFile.createWriter(conf, fileOpt, keyClass, valueClass);
	}

	public void append(K key, V value) throws IOException {
		keyM.set(key); valueM.set(value);
		tmpWriter.append(keyM.writable, valueM.writable);
	}

	public void append(Writable key, Writable value) throws IOException {
		tmpWriter.append(key, value);
	}

	@Override
	public void close() throws IOException {
		if (tmpPath != null && outputPath != null) {
			Path localTmp = tmpPath, localOut = outputPath;
			tmpPath = outputPath = null;
			tmpWriter.close();
			outputLocker.close();
			// may race here
			fs.rename(localTmp, localOut);
		}
	}
}
