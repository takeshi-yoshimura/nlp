package ac.keio.sslab.nlp;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.bottomup.MergingMergedDumper;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class BottomUpDumpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "bottomupDump";
	}

	@Override
	public String getJobDescription() {
		return "generate .csv & .dot files for a bottomup job result";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("b", "bottomupID", true, "ID of bottomup"));
		OptionGroup g2 = new OptionGroup();
		g2.addOption(new Option("l", "ldaID", true, "ID of lda"));
		g.setRequired(true);
		g2.setRequired(true);

		Options opt = new Options();
		opt.addOption("s", "startID", true, "the root of output dendrogram (default: root)");
		opt.addOption("n", "numHierarchy", true, "the number of Hierarchy of output dendrogram (default: 5)");
		opt.addOptionGroup(g);
		opt.addOptionGroup(g2);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		File localOutputDir = mgr.getLocalArgFile(conf.localBottomupFile, "b");
		File mergingMergedFile = new File(localOutputDir.getAbsolutePath(), "mergingToFrom.seq");
		try {
			Map<Integer, String> topics = new HashMap<Integer, String>();
			for (Entry<Integer, List<String>> e: new TopicReader(ldaFiles.dictionaryPath, ldaFiles.topicPath, conf.hdfs, 2).getTopics().entrySet()) {
				topics.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
			}
			MergingMergedDumper dumper = new MergingMergedDumper(mergingMergedFile, conf.localfs, ldaFiles.documentPath, conf.hdfs);
			dumper.dumpCSV(localOutputDir, topics);
			dumper.dumpDot(localOutputDir, topics, mgr.getArgOrDefault("s", dumper.getRoot().ID, Integer.class), mgr.getArgOrDefault("n", 5, Integer.class));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
