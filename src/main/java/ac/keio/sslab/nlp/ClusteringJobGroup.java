package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.hadoop.fs.Path;

public class ClusteringJobGroup implements NLPJobGroup {

	@Override
	final public String getJobName() {
		return "clustering;";
	}

	@Override
	final public String getShortJobName() {
		return "cl";
	}

	@Override
	final public NLPJobGroup getParentJobGroup() {
		return new LDAJob();
	}

	@Override
	final public File getLocalJobDir() {
		return new File(NLPConf.getInstance().localRootFile, getJobName());
	}

	@Override
	final public Path getHDFSJobDir() {
		return new Path(NLPConf.getInstance().rootPath, getJobName());
	}

	@Override
	public String getJobDescription() {
		return "clustering after lda";
	}
}
