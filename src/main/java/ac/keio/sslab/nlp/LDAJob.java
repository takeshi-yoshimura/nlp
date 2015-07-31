package ac.keio.sslab.nlp;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.lda.CVB0;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.RowId;
import ac.keio.sslab.nlp.lda.Seq2sparse;

public class LDAJob implements NLPJob {
	
	protected NLPConf conf = NLPConf.getInstance();

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
		Options options = new Options();
		options.addOption("c", "corpusID", true, "ID of a corpus.");
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
	public void run(Map<String, String> args) {
		Path outputPath = new Path(conf.ldaPath, args.get("j"));
		int numTopics = 300;
		if (args.containsKey("t")) {
			numTopics = Integer.parseInt(args.get("t"));
		}
		int numIterations = 1000;
		if (args.containsKey("x")) {
			numIterations = Integer.parseInt(args.get("x"));
		}
		if (!args.containsKey("c")) {
			System.err.println("Need to specify --corpusID (or -c)");
			return;
		}
		Path corpusPath = new Path(conf.corpusPath, args.get("c"));

		int numMappers = 20;
		if (args.containsKey("nM")) {
			numMappers = Integer.parseInt(args.get("nM"));
		}
		int numReducers = 15;
		if (args.containsKey("nR")) {
			numReducers = Integer.parseInt(args.get("nR"));
		}

		FileSystem fs = null;
		try {
			fs = FileSystem.get(new Configuration());
			if (args.containsKey("ow")) {
				if (!JobUtils.promptDeleteDirectory(fs, outputPath, args.containsKey("force"))) {
					return;
				}
			}
			//No else becaues we want LDA to be restartable.
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
