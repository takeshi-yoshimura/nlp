package ac.keio.sslab.nlp.lda;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

public class Seq2sparse extends RestartableLDAJob {

	protected Path corpusPath;

	public Seq2sparse(FileSystem fs, LDAHDFSFiles hdfs) {
		super(fs, hdfs);
	}

	@Override
	protected AbstractJob getMahoutJobInstance() {
		return new SparseVectorsFromSequenceFiles();
	}

	@Override
	protected boolean resultExists() throws Exception {
		return fs.exists(hdfs.dictionaryPath) && fs.exists(hdfs.tfPath);
	}

	@Override
	protected void setup() throws Exception {
		if (!fs.exists(corpusPath)) {
			throw new Exception("corpus directory " + corpusPath + " does not exist");
		}
	}

	@Override
	protected String[] arguments(Path corpusPath, int numLDATopics, int numLDAIterations) {
		this.corpusPath = corpusPath;
		return new String [] {
				"--input", corpusPath.toString(),
				"--output", hdfs.sparsePath.toString(),
				"--analyzerName", "org.apache.lucene.analysis.core.WhitespaceAnalyzer",
				"--chunkSize", "32",
				"--numReducers", "10",
				"--overwrite",
		};
	}

	@Override
	public void recover() {
		try {
			if (fs.exists(hdfs.sparsePath)) {
				fs.delete(hdfs.sparsePath, true);
			}
		} catch (Exception e) {
			System.err.println("Deleting " + hdfs.sparsePath + " fails: " + e.toString());
			System.err.println("Try deleting " + hdfs.sparsePath + " by manual.");
		}
	}

	@Override
	protected String getJobName() {
		return LDAFiles.sparseDirName;
	}
}
