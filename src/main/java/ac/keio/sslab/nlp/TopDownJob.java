package ac.keio.sslab.nlp;

import java.util.Map;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.clustering.topdown.Main;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;

public class TopDownJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();

	@Override
	public String getJobName() {
		return "topdown";
	}

	@Override
	public String getJobDescription() {
		return "topdown kmeans";
	}

	@Override
	public Options getOptions() {
		Options opt = new Options();
		opt.addOption("l", "ldaID", true, "ID of lda");
		return opt;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("l")) {
			System.err.println("Need to specify --ldaID");
			return;
		}
		LDAHDFSFiles hdfs = new LDAHDFSFiles(new Path(conf.ldaPath, args.get("l")));
		Main topdown = new Main();
		try {
			topdown.run(hdfs.documentPath, new Path(conf.topdownPath, args.get("j")), 32);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error during clustering");
		}
	}

	@Override
	public void takeSnapshot() {
	}

	@Override
	public void restoreSnapshot() {
	}

	@Override
	public boolean runInBackground() {
		return true;
	}

}
