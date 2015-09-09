package ac.keio.sslab.nlp;

import java.io.File;
import java.util.Date;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.clustering.view.GALowerClassifier;

public class GALowerClassJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "bottomup.galower";
	}

	@Override
	public String getShortJobName() {
		return "gauc";
	}

	@Override
	public NLPJobGroup getParentJobGroup() {
		return new BottomUpJob();
	}

	@Override
	public File getLocalJobDir() {
		return new File(NLPConf.getInstance().finalOutputFile, "class");
	}

	@Override
	public Path getHDFSJobDir() {
		return null;
	}

	@Override
	public String getJobDescription() {
		return "extract similar patches from bottomup result lower than specified ga";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("ga", "groupAverage", true, "output clusters whose ga is lower than this value"));
		g.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		return opt;
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		File outputDir = mgr.getLocalOutputDir();
		File clustersFile = new File(pMgr.getLocalOutputDir(), "clusters.csv");

		System.out.println("Start at: " + new Date().toString());
		outputDir.getParentFile().mkdirs();
		System.out.println("loading " + clustersFile.getAbsolutePath());
		GALowerClassifier c = new GALowerClassifier(clustersFile);
		double ga = mgr.getArgOrDefault("ga", 1.0, Double.class);
		c.writeResultCSV(outputDir, ga);
        System.out.println("Results: " + outputDir.getAbsolutePath());
		System.out.println("End at:" + new Date().toString());
	}

	@Override
	public boolean runInBackground() {
		return true;
	}
}
