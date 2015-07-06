package ac.keio.sslab.nlp.lda;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Seq2sparseSnapshot extends LDASnapshotJob {

	public Seq2sparseSnapshot(FileSystem fs, LDAHDFSFiles hdfs, LDALocalFiles local) {
		super(fs, hdfs, local);
	}

	@Override
	public void takeSnapshot() {
		//Check if the directory layout is legal
		Map<Path, String> checkMap = new HashMap<Path, String>();
		checkMap.put(hdfs.tfPath, "TF-vectors directory");
		checkMap.put(hdfs.dictionaryPath, "Dictionary file");
		if (!pathsExist(checkMap)) {
			return;
		}

		//List up files to be copied
		Set<Path> copySet = new HashSet<>();
		copySet.add(hdfs.tfPath);
		copySet.add(hdfs.dictionaryPath);
		//Copy the files from HDFS to a local disk
		copyFromHDFSToLocalFS(copySet);
	}

	@Override
	public void restoreSnapshot() {
		//Check the snapshot in a local disk
		Map<File, String> checkMap = new HashMap<File, String>();
		checkMap.put(local.tfFile, "TF-vectors directory");
		checkMap.put(local.dictionaryFile, "Dictionary file");
		if (!filesExist(checkMap)) {
			return;
		}

		//Copy the files from a local disk to HDFS
		Set<File> set = new HashSet<File>();
		set.add(local.tfFile);
		set.add(local.dictionaryFile);
		copyFromLocalFSToHDFS(set);
	}

	@Override
	protected String getJobName() {
		return LDAFiles.sparseDirName;
	}

}
