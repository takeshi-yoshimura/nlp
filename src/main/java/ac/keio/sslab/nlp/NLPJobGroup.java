package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.hadoop.fs.Path;

public interface NLPJobGroup {
	public abstract String getJobName();
	public abstract String getShortJobName();
	public abstract NLPJobGroup getParentJobGroup();
	public abstract File getLocalJobDir();
	public abstract Path getHDFSJobDir();
	public abstract String getJobDescription();
}
