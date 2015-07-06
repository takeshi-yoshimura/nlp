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

public class SequenceDirectoryReader {

	FileSystem fs;
	Configuration conf;
	List<Path> files;
	int currentIndex;
	SequenceFile.Reader reader;

	public SequenceDirectoryReader(Path dir, Configuration conf) throws FileNotFoundException, IOException {
		FileSystem fs = dir.getFileSystem(conf);
		this.fs = fs;
		this.conf = conf;

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

	public boolean next(Writable key, Writable val) throws IOException {
		boolean result = reader.next(key, val);
		if (result == false &&  currentIndex + 1 < files.size()) {
			currentIndex++;
			reader.close();
			Reader.Option fileOpt = Reader.file(files.get(currentIndex));
			reader = new Reader(fs.getConf(), fileOpt);
			result = reader.next(key, val);
		}
		return result;
	}
	
	public void close() throws IOException {
		reader.close();
	}
}
