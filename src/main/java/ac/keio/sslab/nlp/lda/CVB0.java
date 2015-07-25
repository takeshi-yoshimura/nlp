package ac.keio.sslab.nlp.lda;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobFilter;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.lda.cvb.CVB0Driver;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;
import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.nlp.NLPConf;

public class CVB0 extends RestartableLDAJob {

	int numMappers, numReducers;
	NLPConf conf = NLPConf.getInstance();

	public CVB0(FileSystem fs, LDAHDFSFiles hdfs, int numMappers, int numReducers) {
		super(fs, hdfs);
		this.numMappers = numMappers;
		this.numReducers = numReducers;
	}

	@Override
	protected AbstractJob getMahoutJobInstance() {
		return new CVB0Driver();
	}

	@Override
	protected boolean resultExists() throws Exception {
		return JobUtils.isSucceededMapReduce(fs, hdfs.topicPath) && JobUtils.isSucceededMapReduce(fs, hdfs.documentPath);
	}

	@Override
	public void postRun(Path corpusPath, int numLDATopics, int numLDAIterations) throws Exception {
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(hdfs.matrixPath, fs.getConf());

		long size = 0;
		while (reader.seekNext()) {
			size++;
		}
		reader.close();
		if (fs.exists(hdfs.splitMatrixPath)) {
			fs.delete(hdfs.splitMatrixPath, true);
		}
		fs.mkdirs(hdfs.splitMatrixPath);
		int chunks = 1;
		reader = new SequenceDirectoryReader<>(hdfs.matrixPath, fs.getConf());
		SequenceSwapWriter<Integer, Vector> writer = new SequenceSwapWriter<>(new Path(hdfs.splitMatrixPath, "chunk-" + chunks), conf.tmpPath, fs.getConf(), true);
		long count = 0;
		while (reader.seekNext()) {
			if (count++ > size / numMappers) {
				writer.close();
				chunks++;
				count = 0;
				writer = new SequenceSwapWriter<>(new Path(hdfs.splitMatrixPath, "chunk-" + chunks), conf.tmpPath, fs.getConf(), true);
			}
			writer.append(reader.keyW(), reader.valW());
		}
		writer.close();
		reader.close();
	}

	@Override
	protected String[] arguments(Path corpusPath, int numLDATopics, int numLDAIterations) {
		return new String [] {
				"--input", hdfs.splitMatrixPath.toString(),
				"--dictionary", hdfs.dictionaryPath.toString(),
				"--output", hdfs.topicPath.toString(),
				"--doc_topic_output", hdfs.documentPath.toString(),
				"--topic_model_temp_dir", hdfs.modelPath.toString(),

				"--num_topics", Integer.toString(numLDATopics),
				"--maxIter", Integer.toString(numLDAIterations),
				"--doc_topic_smoothing", Double.toString(50 / numLDATopics),
				"--term_topic_smoothing", Double.toString(0.1),

				"--iteration_block_size", Integer.toString(10),
				"--test_set_fraction", Double.toString(0.1),
				"--random_seed", Long.toString(System.nanoTime() % 10000),
				"--num_reduce_tasks", Integer.toString(numReducers),
		};
	}

	@Override
	protected void setup() throws Exception {
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
