package ac.keio.sslab.nlp;

import java.io.File;
import java.util.Date;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.clustering.view.TopdownClassifier;

public class GAUnderClassJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "class.gaunder";
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
		return "extract similar patches from bottomup result under specified ga";
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
	public void run(JobManager mgr) throws Exception {
		File outputDir = mgr.getLocalOutputDir();
		File clustersFile = new File(mgr.getParentJobManager().getLocalOutputDir(), "clusters.csv");

		System.out.println("Start at: " + new Date().toString());
		outputDir.getParentFile().mkdirs();
		System.out.println("loading " + clustersFile.getAbsolutePath());
		TopdownClassifier c = new TopdownClassifier(clustersFile);
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
