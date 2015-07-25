package ac.keio.sslab.hadoop.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.Reader;

public class SequenceDirectoryReader<K, V> {

	FileSystem fs;
	Configuration conf;
	List<Path> files;
	int currentIndex;
	SequenceFile.Reader reader;
	
	WritableMediator<K, ? extends Writable> keyM;
	WritableMediator<V, ? extends Writable> valueM;

	public SequenceDirectoryReader(Path dir, Configuration conf) throws FileNotFoundException, IOException {
		FileSystem fs = dir.getFileSystem(conf);
		this.fs = fs;
		this.conf = conf;

		try {
			WritableMediatorFactory<K> keyFactory = new WritableMediatorFactory<K>();
			keyM = keyFactory.getMediator();
			WritableMediatorFactory<V> valueFactory = new WritableMediatorFactory<V>();
			valueM = valueFactory.getMediator();
		} catch (Exception e) {
			throw new IOException("Instantiation failure");
		}

		files = new ArrayList<Path>();
		for (FileStatus status: fs.listStatus(dir)) {
			if (status.isDirectory() || status.getLen() == 0) //avoid reading _SUCCESS
				continue;
			files.add(status.getPath());
		}
		if (files.size() == 0) {
			throw new FileNotFoundException("Not found any files in " + dir);
		}
		currentIndex = 0;
		Reader.Option fileOpt = Reader.file(files.get(0));
		reader = new Reader(fs.getConf(), fileOpt);
	}

	public boolean seekNext() throws IOException {
		boolean result = reader.next(keyM.writable, valueM.writable);
		if (result == false &&  currentIndex + 1 < files.size()) {
			currentIndex++;
			reader.close();
			Reader.Option fileOpt = Reader.file(files.get(currentIndex));
			reader = new Reader(fs.getConf(), fileOpt);
			result = reader.next(keyM.writable, valueM.writable);
		}
		return result;
	}

	public K key() {
		return keyM.get();
	}

	public Writable keyW() {
		return keyM.writable;
	}

	public Writable valW() {
		return valueM.writable;
	}

	public V val() {
		return valueM.get();
	}
	
	public void close() throws IOException {
		reader.close();
	}
}
