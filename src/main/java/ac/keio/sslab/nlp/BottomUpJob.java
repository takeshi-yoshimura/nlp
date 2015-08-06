package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.clustering.bottomup.BottomupClustering;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;

public class BottomUpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "bottomup";
	}

	@Override
	public String getJobDescription() {
		return "hierarchical clustering";
	}

	@Override
	public Options getOptions() {
		OptionGroup required = new OptionGroup();
		required.addOption(new Option("l", "ldaID", true, "ID of lda"));
		OptionGroup required2 = new OptionGroup();
		required2.addOption(new Option("d", "distanceMeasure", true, "distance measure name (Cosine or Euclidean)"));
		required.setRequired(true);
		required2.setRequired(true);

		Options opt = new Options();
		opt.addOption("t", "threashold", true, "bytes of threashold to optimize algorithm. K, M, G postfix are available (default: 6.4G)");
		opt.addOption("m", "multiThreaded", false, "use multi threads");
		opt.addOptionGroup(required);
		opt.addOptionGroup(required2);
		return opt;
	}

	public long parseThreashold(String str) {
		long postfix = 1;
		str = str.toLowerCase();
		if (str.matches("^.*[mgk]$")) {
			switch(str.charAt(str.length() - 1)) {
			case 'k': postfix = 1024; break;
			case 'm': postfix = 1024 * 1024; break;
			case 'g': postfix = 1024 * 1024 * 1024; break;
			}
			str = str.substring(0, str.length() - 1);
		}
		double d = Double.parseDouble(str);
		return (long)(d * postfix);
	}

	@Override
	public void run(JobManager mgr) {
		long threashold = parseThreashold(mgr.getArgOrDefault("t", "6.4G", String.class));
		boolean threaded = mgr.getArgOrDefault("m", false, Boolean.class);

		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		File localOutputDir = new File(conf.finalOutputFile, "bottomup/" + mgr.getJobID());
		Path mergingMergedPath = new Path(localOutputDir.getAbsolutePath(), "mergingToFrom.seq"); //Note: local FS

		try {
			BottomupClustering bc = new BottomupClustering(ldaFiles.documentPath, conf.hdfs, mgr.getArgStr("d"), threashold, threaded);
			if (conf.hdfs.exists(mergingMergedPath) && !mgr.doForceWrite()) {
				bc.restore(mergingMergedPath, conf.hdfs);
			}
			bc.run(mergingMergedPath, conf.hdfs, mgr.doForceWrite());
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	@Override
	public boolean runInBackground() {
		return true;
	}

}
