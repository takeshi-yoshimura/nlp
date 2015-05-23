package ac.keio.sslab.nlp;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.lda.CVB0;
import ac.keio.sslab.nlp.lda.CVB0Snapshot;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.LDALocalFiles;
import ac.keio.sslab.nlp.lda.RowId;
import ac.keio.sslab.nlp.lda.RowIdSnapshot;
import ac.keio.sslab.nlp.lda.Seq2sparse;
import ac.keio.sslab.nlp.lda.Seq2sparseSnapshot;

public class LDAJob implements NLPJob {
	
	protected final NLPConf conf = new NLPConf();

	@Override
	public String getJobName() {
		return NLPConf.LDAJobName;
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

		CVB0 cvb = new CVB0(fs, hdfs);
		try {
			cvb.start(corpusPath, numTopics, numIterations);
		} catch (Exception e) {
			System.err.println("CVB0 failed: " + e.toString());
		}
	}

	@Override
	public void takeSnapshot() {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			conf.localLdaFile.mkdir();
			for (FileStatus stat: fs.listStatus(conf.ldaPath)) {
				LDAHDFSFiles hdfs = new LDAHDFSFiles(stat.getPath());
				LDALocalFiles local = new LDALocalFiles(new File(conf.localLdaFile, stat.getPath().getName()));
				(new Seq2sparseSnapshot(fs, hdfs, local)).takeSnapshot();
				(new RowIdSnapshot(fs, hdfs, local)).takeSnapshot();
				(new CVB0Snapshot(fs, hdfs, local)).takeSnapshot();
			}
		} catch (IOException e) {
			System.err.println("Taking snapshot failed at " + getJobName() + ": " + e.toString());
		}
	}

	@Override
	public void restoreSnapshot() {
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			fs.mkdirs(conf.ldaPath);
			for (File localOutputFile: conf.localLdaFile.listFiles()) {
				LDAHDFSFiles hdfs = new LDAHDFSFiles(new Path(conf.ldaPath, localOutputFile.getName()));
				LDALocalFiles local = new LDALocalFiles(localOutputFile);
				(new Seq2sparseSnapshot(fs, hdfs, local)).restoreSnapshot();
				(new RowIdSnapshot(fs, hdfs, local)).restoreSnapshot();
				(new CVB0Snapshot(fs, hdfs, local)).restoreSnapshot();
			}
		} catch (IOException e) {
			System.err.println("Restoring snapshot failed at " + getJobName() + ": " + e.toString());
		}
	}

	@Override
	public boolean runInBackground() {
		return true;
	}
}
