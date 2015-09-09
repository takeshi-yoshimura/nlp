package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.lda.CVB0;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.LocalCVB0;
import ac.keio.sslab.nlp.lda.RowId;
import ac.keio.sslab.nlp.lda.Seq2sparse;

public class LDAJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "lda";
	}

	@Override
	public String getShortJobName() {
		return "l";
	}

	@Override
	public NLPJobGroup getParentJobGroup() {
		return new CorpusJobGroup();
	}

	@Override
	public File getLocalJobDir() {
		return new File(NLPConf.getInstance().localRootFile, getJobName());
	}

	@Override
	public Path getHDFSJobDir() {
		return new Path(NLPConf.getInstance().rootPath, getJobName());
	}

	@Override
	public String getJobDescription() {
		return "Run *restartable* topic modeling with Mahout";
	}

	@Override
	public Options getOptions() {
		Options options = new Options();
		options.addOption("t", "numTopics", true, "Number of topics. Default is 300.");
		options.addOption("x", "numIterations", true, "Number of iterations. Default is 1000.");
		options.addOption("nM", "numMappers", true, "Numer of Mappers in CVB0. Default is 20.");
		options.addOption("nR", "numReducers", true, "Number of Reducers in CVB0. Default is 15.");
		options.addOption("loc", "local-only", false, "Local execution of LDA");
		/* Do not allow to change other smoothing parameters of LDA.
		 * Because they confuse users and it is hard for users to recognize the effect.
		 * Note that LDA itself cannot discover the perfect parameters (discovers only local-minimized, likelyhood results).
		 */
		return options;
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		NLPConf conf = NLPConf.getInstance();
		Path outputPath = mgr.getHDFSOutputDir();
		int numTopics = mgr.getArgOrDefault("t", 300, Integer.class);
		int numIterations = mgr.getArgOrDefault("x", 1000, Integer.class);
		Path corpusPath = pMgr.getHDFSOutputDir();
		int numMappers = mgr.getArgOrDefault("nM", 20, Integer.class);
		int numReducers = mgr.getArgOrDefault("nR", 15, Integer.class);
		boolean local = mgr.hasArg("loc");

		LDAHDFSFiles hdfs = new LDAHDFSFiles(outputPath);
		new Seq2sparse(conf.hdfs, hdfs).start(corpusPath, numTopics, numIterations);
		new RowId(conf.hdfs, hdfs).start(corpusPath, numTopics, numIterations);
		if (!local) {
			new CVB0(conf.hdfs, hdfs, numMappers, numReducers).start(corpusPath, numTopics, numIterations);
		} else {
			new LocalCVB0(conf.hdfs, hdfs, numMappers, numReducers).start(corpusPath, numTopics, numIterations);	
		}
	}

	@Override
	public boolean runInBackground() {
		return true;
	}
}
