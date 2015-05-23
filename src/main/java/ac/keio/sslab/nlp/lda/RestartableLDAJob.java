package ac.keio.sslab.nlp.lda;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.AbstractJob;

import ac.keio.sslab.nlp.JobUtils;

public abstract class RestartableLDAJob {
	protected abstract AbstractJob getMahoutJobInstance();
	protected abstract boolean resultExists() throws Exception;
	protected abstract String [] arguments(Path gitPath, int numLDATopics, int numLDAIterations);
	protected abstract void setup() throws Exception;
	public abstract void recover();
	protected abstract String getJobName();

	protected FileSystem fs;
	protected LDAHDFSFiles hdfs;
	protected Path outputPath;
	private Path argPath;

	public RestartableLDAJob(FileSystem fs, LDAHDFSFiles hdfs) {
		this.fs = fs;
		this.hdfs = hdfs;
		switch (getJobName()) {
			case LDAFiles.cvbJobName: outputPath = hdfs.cvbPath; break;
			case LDAFiles.rowIdJobName: outputPath = hdfs.rowIdPath; break;
			case LDAFiles.sparseJobName: outputPath = hdfs.sparsePath; break;
		}
		argPath = new Path(outputPath, LDAFiles.argumentFileName);
	}

	public void start(Path corpusPath, int numLDATopics, int numLDAIterations) throws Exception {
		String [] args = null;
		try {
			if (resultExists()) {
				return;
			}

			if (fs.exists(argPath)) {
				args = JobUtils.restoreArguments(fs, argPath);
			} else {
				args = arguments(corpusPath, numLDATopics, numLDAIterations);
				JobUtils.saveArguments(fs, argPath, args);
			}
			setup();
		} catch (Exception e) {
			throw new Exception("Preparing " + getJobName() + " failed.");
		}

		try {
			getMahoutJobInstance().run(args);
		} catch (Exception e) {
			recover();
			e.printStackTrace();
			throw new Exception(getJobName() + " failed.");
		}
	}
}
