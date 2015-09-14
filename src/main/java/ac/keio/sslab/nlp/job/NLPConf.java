package ac.keio.sslab.nlp.job;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jettison.json.JSONObject;

public class NLPConf {

	public Configuration hadoopConf;
	public FileSystem hdfs;
	public Path rootPath;
	public Path tmpPath;

	public FileSystem localfs;
	public File localRootFile;
	public File localLogFile;
	public File localBackupFile;
	public File localArgFile;
	public File localLockFile;
	public File localTmpFile;

	public File finalOutputFile;
	public File finalStableCommitStatsFile;

	public String runPath = "/var/lib/nlp/run";
	public static String nlpHDFSDirName = "/nlp/";
	public static String nlpLocalDirName = "/var/lib/nlp";
	public static final String tmpDirName = "tmp";
	public static final String logDirName = "log";
	public static final String backupDirName = "backup";

	public static final String finalOutputDirName = "final";
	public static final String finalStableCommitStatsFileName = "stable-commits";

	public static final String argDirName = "ARGUMENT";
	public static final String lockDirName = "LOCK";

	public static final int maxTopicTerms = 10;

	protected static NLPConf instance = new NLPConf();
	public static NLPConf getInstance() {
		return instance;
	}

	protected void resetConf() {
		rootPath = new Path(nlpHDFSDirName);
		tmpPath = new Path(rootPath, tmpDirName);

		localRootFile = new File(nlpLocalDirName);
		localLogFile = new File(localRootFile, logDirName);
		localBackupFile = new File(localRootFile, backupDirName);
		localArgFile = new File(localRootFile, argDirName);
		localLockFile = new File(localRootFile, lockDirName);
		localTmpFile = new File(localRootFile, tmpDirName);

		finalOutputFile = new File(localRootFile, finalOutputDirName);
		finalStableCommitStatsFile = new File(finalOutputFile, finalStableCommitStatsFileName);
	}

	public void loadConfFile(String confFileName) {
		try {
			FileInputStream inputStream = new FileInputStream(confFileName);
			JSONObject jobJson = new JSONObject(IOUtils.toString(inputStream));
			String tmpNlpHDFSDirName = nlpHDFSDirName, tmpNlpLocalDirName = nlpLocalDirName, tmpRunPath = runPath;
			if (jobJson.has("nlpHDFSDirName")) {
				tmpNlpHDFSDirName = jobJson.getString("nlpHDFSDirName");
			}
			if (jobJson.has("nlpLocalDirName")) {
				tmpNlpLocalDirName = jobJson.getString("nlpLocalDirName");
			}
			if (jobJson.has("runPath")) {
				tmpRunPath = jobJson.getString("runPath");
			}
			inputStream.close();
			nlpHDFSDirName = tmpNlpHDFSDirName;
			nlpLocalDirName = tmpNlpLocalDirName;
			runPath = tmpRunPath;
		} catch (Exception e) {
			System.err.println("Reading json " + confFileName + " failed: " + e.toString());
			System.err.println("use default values.");
		}
		resetConf();
	}

	private NLPConf() {
		hadoopConf = new Configuration();
		try {
			hdfs = FileSystem.get(hadoopConf);
			localfs = FileSystem.getLocal(hadoopConf);
		} catch (Exception e) {
			System.err.println("!!!!!!WARNING: Loading hdfs or local fs failed!!!!!");
			hdfs = localfs = null;
		}
		resetConf();
	}
}
