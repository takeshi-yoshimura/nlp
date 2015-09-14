package ac.keio.sslab.nlp.job;

import java.io.File;

import org.apache.hadoop.fs.Path;

public class CorpusJobGroup implements NLPJobGroup {

	@Override
	final public String getJobName() {
		return "corpus";
	}

	@Override
	final public String getShortJobName() {
		return "c";
	}

	@Override
	final public NLPJobGroup getParentJobGroup() {
		return null;
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
		return "load, filter, and upload corpus";
	}
}
