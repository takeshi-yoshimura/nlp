package ac.keio.sslab.nlp;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.topdown.Main;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;

public class TopDownJob implements NLPJob {

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
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("l", "ldaID", true, "ID of lda"));
		g.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		LDAHDFSFiles hdfs = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		Main topdown = new Main();
		try {
			topdown.run(hdfs.documentPath, mgr.getJobIDPath(conf.topdownPath), 32);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error during clustering");
		}
	}

	@Override
	public boolean runInBackground() {
		return true;
	}

}
