package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.hadoop.fs.Path;

//TODO: use Java property
public class NLPConf {

	public final Path rootPath;
	public final File localRootFile;
	public final Path ldaPath;
	public final Path corpusPath;
	public final Path topdownPath;
	public final Path tmpPath;

	public final File localLogFile;
	public final File localLdaFile;
	public final File localGitFile;
	public final File localCorpusFile;
	public final File localArgFile;
	public final File localLockFile;
	public final File localTmpFile;

	public final File finalOutputFile;
	public final File finalStableCommitStatsFile;

	public static final String nlpDirName = "/user/yos/nlp/";
	public static final String nlpLocalDirName = "/var/lib/nlp";
	public static final String linuxPatchFile = "linux_patch";
	public static final String tmpDirName = "/tmp/nlp/";
	public static final String logDirName = "log";
	public static final String LDADirName = "lda";
	public static final String gitDirName = "git";
	public static final String tpdownDirName = "topdown";
	public static final String corpusDirName = "corpus";
	
	public static final String finalOutputDirName = "final";
	public static final String finalStableCommitStatsFileName = "stable-commits";

	public static final String gitSetupJobName = "gitSetup";
	public static final String LDAJobName = "lda";
	public static final String gitCorpusJobName = "gitCorpus";
	public static final String linuxPatchJobName = "linuxPatch";
	public static final String snapshotJobName = "snapshot";
	public static final String argDirName = "ARGUMENT";
	public static final String lockDirName = "LOCK";

	public static final String numCAnalyzerThreads = "16";
	public static final int maxTopicTerms = 10;

	public NLPConf() {
		rootPath = new Path(nlpDirName);
		localRootFile = new File(nlpLocalDirName);
		ldaPath = new Path(rootPath, LDADirName);
		corpusPath = new Path(rootPath, corpusDirName);
		topdownPath = new Path(rootPath, tpdownDirName);
		tmpPath = new Path(tmpDirName);

		localLogFile = new File(localRootFile, logDirName);
		localLdaFile = new File(localRootFile, LDADirName);
		localGitFile = new File(localRootFile, gitDirName);
		localCorpusFile = new File(localRootFile, corpusDirName);
		localArgFile = new File(localRootFile, argDirName);
		localLockFile = new File(localRootFile, lockDirName);
		localTmpFile = new File(tmpDirName);
		
		finalOutputFile = new File(localRootFile, finalOutputDirName);
		finalStableCommitStatsFile = new File(finalOutputFile, finalStableCommitStatsFileName);
	}
}
