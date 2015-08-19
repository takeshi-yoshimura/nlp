package ac.keio.sslab.nlp;

import java.io.File;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;

import ac.keio.sslab.clustering.bottomup.BottomupClustering;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;

public class BottomUpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "bottomup";
	}

	@Override
	public String getJobDescription() {
		return "group average hierarchical clustering";
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
		opt.addOptionGroup(required);
		opt.addOptionGroup(required2);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		File localOutputDir = new File(conf.finalOutputFile, "bottomup/" + mgr.getJobID());
		File mergingMergedPath = new File(localOutputDir.getAbsolutePath(), "mergingToFrom.seq"); //Note: local FS

		String distance = mgr.getArgStr("d");
		DistanceMeasure measure;
		if (distance.toLowerCase().equals("cosine")) {
			measure = new CosineDistanceMeasure();
		} else if (distance.toLowerCase().equals("euclidean")) {
			measure = new SquaredEuclideanDistanceMeasure();
		} else {
			System.err.println("Specify Cosine or Euclidean ");
			return;
		}

		try {
			BottomupClustering bc = new BottomupClustering(ldaFiles.documentPath, conf.hdfs, measure);
			bc.run(mergingMergedPath, mgr.doForceWrite());
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
