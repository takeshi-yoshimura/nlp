package ac.keio.sslab.nlp.lda;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.utils.vectors.RowIdJob;

import ac.keio.sslab.nlp.JobUtils;

public class RowId extends RestartableLDAJob {

	public RowId(FileSystem fs, LDAHDFSFiles hdfs) {
		super(fs, hdfs);
	}

	@Override
	protected AbstractJob getMahoutJobInstance() {
		return new RowIdJob();
	}

	@Override
	protected boolean resultExists() throws Exception {
		return fs.exists(hdfs.matrixPath) && fs.exists(hdfs.docIndexPath);
	}

	@Override
	protected String[] arguments(Path corpusPath, int numLDATopics, int numLDAIterations) {
		return new String [] {
				"--input", hdfs.tfPath.toString(),
				"--output", hdfs.rowIdPath.toString(),
		};
	}

	@Override
	protected void setup() throws Exception {
		if (!JobUtils.isSucceededMapReduce(fs, hdfs.tfPath)) {
			throw new IOException("sparse/tf-vectors does not exist or be inconsistent. Delete them and try lda again.");
		}
	}

	@Override
	public void recover() {
		try {
			if (fs.exists(hdfs.rowIdPath)) {
				fs.delete(hdfs.rowIdPath, true);
			}
		} catch (Exception e2) {
			System.err.println("Deleting " + hdfs.rowIdPath + " fails: " + e2.toString());
			System.err.println("Try deleting " + hdfs.rowIdPath + " by manual.");
		}
	}

	@Override
	protected String getJobName() {
		return LDAFiles.rowIdDirName;
	}
}
