package ac.keio.sslab.nlp;

import java.io.IOException;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.lda.CVB0;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.RowId;
import ac.keio.sslab.nlp.lda.Seq2sparse;

public class LDAJob implements NLPJob {

	@Override
	public String getJobName() {
		return "lda";
	}

	@Override
	public String getJobDescription() {
		return "Run *restartable* topic modeling with Mahout";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("c", "corpusID", true, "ID of a corpus."));
		g.setRequired(true);

		Options options = new Options();
		options.addOptionGroup(g);
		options.addOption("t", "numTopics", true, "Number of topics. Default is 300.");
		options.addOption("x", "numIterations", true, "Number of iterations. Default is 1000.");
		options.addOption("nM", "numMappers", true, "Numer of Mappers in CVB0. Default is 20.");
		options.addOption("nR", "numReducers", true, "Number of Reducers in CVB0. Default is 15.");
		/* Do not allow to change other smoothing parameters of LDA.
		 * Because they confuse users and it is hard for users to recognize the effect.
		 * Note that LDA itself cannot discover the perfect parameters (discovers only local-minimized, likelyhood results).
		 */
		return options;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		Path outputPath = mgr.getArgJobIDPath(conf.ldaPath, "j");
		int numTopics = mgr.getArgOrDefault("t", 300, Integer.class);
		int numIterations = mgr.getArgOrDefault("x", 1000, Integer.class);
		Path corpusPath = mgr.getArgJobIDPath(conf.corpusPath, "c");
		int numMappers = mgr.getArgOrDefault("nM", 20, Integer.class);
		int numReducers = mgr.getArgOrDefault("nR", 15, Integer.class);

		FileSystem fs = null;
		try {
			fs = FileSystem.get(new Configuration());
			fs.mkdirs(outputPath);
		} catch (IOException e) {
			System.err.println("Connecting HDFS failed : " + e.toString());
			return;
		}

		LDAHDFSFiles hdfs = new LDAHDFSFiles(outputPath);
		Seq2sparse sparse = new Seq2sparse(fs, hdfs);
		try {
			sparse.start(corpusPath, numTopics, numIterations);
		} catch (Exception e) {
			System.err.println("Seq2sparse failed: " + e.toString());
			return;
		}

		RowId rowid = new RowId(fs, hdfs);
		try {
			rowid.start(corpusPath, numTopics, numIterations);
		} catch (Exception e) {
			System.err.println("RowId failed: " + e.toString());
			return;
		}

		CVB0 cvb = new CVB0(fs, hdfs, numMappers, numReducers);
		try {
			cvb.start(corpusPath, numTopics, numIterations);
		} catch (Exception e) {
			System.err.println("CVB0 failed: " + e.toString());
		}
	}

	@Override
	public boolean runInBackground() {
		return true;
	}
}
