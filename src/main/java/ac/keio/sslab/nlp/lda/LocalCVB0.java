package ac.keio.sslab.nlp.lda;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.lda.cvb.InMemoryCollapsedVariationalBayes0;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.DenseMatrix;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.nlp.NLPConf;

public class LocalCVB0 extends RestartableLDAJob {

	int numMappers, numReducers;
	NLPConf conf = NLPConf.getInstance();

	public LocalCVB0(FileSystem fs, LDAHDFSFiles hdfs, int numMappers, int numReducers) {
		super(fs, hdfs);
		this.numMappers = numMappers;
		this.numReducers = numReducers;
	}

	@Override
	protected AbstractJob getMahoutJobInstance() {
		// need dummy arguments because the default constructor is in private
		return new InMemoryCollapsedVariationalBayes0(new DenseMatrix(1, 1), null, 100, 0.5, 0.1, 1, 1, 0.0);
	}

	@Override
	protected boolean resultExists() throws Exception {
		return JobUtils.isSucceededMapReduce(fs, hdfs.topicPath) && JobUtils.isSucceededMapReduce(fs, hdfs.documentPath);
	}

	@Override
	protected String[] arguments(Path corpusPath, int numLDATopics, int numLDAIterations) {
		return new String [] {
				"--input", hdfs.matrixPath.toString(),
				"--dictionary", hdfs.dictionaryPath.toString(),
				"--topicOutputFile", hdfs.topicPath.toString(),
				"--docOutputFile", hdfs.docIndexPath.toString(),
				"--numTopics", Integer.toString(numLDATopics),
				"--maxIterations", Integer.toString(numLDAIterations),
				"--alpha", Double.toHexString(50 / numLDATopics),
				"--eta", Double.toString(0.1),
				"--numTrainThreads", Integer.toString(12),
				"--numUpdateThreads", Integer.toString(12),
		};
	}

	@Override
	protected void setup(Path corpusPath, int numLDATopics, int numLDAIterations) throws Exception {
		if (!fs.exists(hdfs.cvbPath)) {
			fs.mkdirs(hdfs.cvbPath);
		}
	}

	/**
	 * Keep only files which derive from succeeded MapReduce.
	 * Note that only one directory becomes inconsistent on a MapReduce failure.
	 * Unfortunately, this function cannot hold the exception safety, so try to assist a manual recovery.
	 */
	@Override
	public void recover() {
		try {
			System.err.print("Try to check: " + hdfs.topicPath.toString());
			if (fs.exists(hdfs.topicPath) && !JobUtils.isSucceededMapReduce(fs, hdfs.topicPath)) {
				fs.delete(hdfs.topicPath, true);
				System.err.println(" Broken\nBroken " + hdfs.topicPath + " was deleted");
				return;
			}
			System.err.println(" OK");

			System.err.print("Try to check: " + hdfs.documentPath.toString());
			if (fs.exists(hdfs.documentPath) && !JobUtils.isSucceededMapReduce(fs, hdfs.documentPath)) {
				fs.delete(hdfs.documentPath, true);
				System.err.print(" Broken\nBroken " + hdfs.documentPath + " was deleted");
				return;
			}
			System.err.println(" OK");

			System.err.print("Try to check: " + hdfs.modelPath.toString());
			if (!fs.exists(hdfs.modelPath)){
				return;
			}
			System.err.println(" Exists");

			System.err.print("Try to check: " + hdfs.modelPath.toString() + "/perplexity-XX");
			int foundPerp = 0;
			for (FileStatus stat: fs.listStatus(hdfs.modelPath, new GlobFilter("perplexity-*"))) {
				System.err.print("Try to check: " + stat.getPath().toString());
				if (!JobUtils.isSucceededMapReduce(fs, stat.getPath())) {
					fs.delete(stat.getPath(), true);
					System.err.print(" Broken\nBroken " + stat.getPath() + " was deleted");
					return;
				}
				System.err.println(" OK");
				foundPerp++;
			}
			System.err.println("Found perplexity directories: " + foundPerp);

			System.err.print("Try to find the largest XX in " + hdfs.modelPath.toString() + "/model-XX");
			int numModels = -1;
			for (FileStatus stat: fs.listStatus(hdfs.modelPath, new GlobFilter("model-*"))) {
				Integer modelNumber = Integer.parseInt(stat.getPath().getName().substring("model-".length()));
				if (modelNumber > numModels) {
					numModels = modelNumber;
				}
			}
			if (numModels == -1) {
				System.err.println(" model-XX directory was not found.");
				return;
			}

			Path checkModelPath = new Path(hdfs.modelPath, "model-" + Integer.toString(numModels));
			System.err.print("XX was " + numModels + ". Try to check: " + checkModelPath.toString());
			if (!JobUtils.isSucceededMapReduce(fs, checkModelPath)) {
				fs.delete(checkModelPath, true);
				System.err.print(" Broken\nBroken " + checkModelPath + " was deleted");
			}
			System.err.println(" OK");
		} catch (Exception e) {
			System.err.println("Recovering the directory " + hdfs.cvbPath.toString() + " failed: " + e.toString());
			System.err.println("Try fixing the directory by manual.");
		}
	}

	@Override
	protected String getJobName() {
		return LDAFiles.cvbJobName;
	}
}
