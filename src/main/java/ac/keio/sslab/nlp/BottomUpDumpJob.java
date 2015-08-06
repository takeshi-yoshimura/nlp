package ac.keio.sslab.nlp;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.clustering.bottomup.MergingMergedDumper;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class BottomUpDumpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "bottomup";
	}

	@Override
	public String getJobDescription() {
		return "generate a .dot file for a dendrogram with a topdown result";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("b", "bottomupID", true, "ID of bottomup"));
		g.addOption(new Option("l", "ldaID", true, "ID of lda"));
		g.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		File localOutputDir = new File(conf.finalOutputFile, "bottomup/" + mgr.getArgStr("b"));
		Path mergingMergedPath = new Path(localOutputDir.getAbsolutePath(), "mergingToFrom.seq");
		try {
			Map<Integer, String> topics = new HashMap<Integer, String>();
			for (Entry<Integer, List<String>> e: new TopicReader(ldaFiles.dictionaryPath, ldaFiles.topicPath, conf.hadoopConf, 2).getTopics().entrySet()) {
				topics.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
			}
			MergingMergedDumper dumper = new MergingMergedDumper(ldaFiles.documentPath, conf.hdfs, mergingMergedPath, conf.localfs);
			dumper.dumpCSV(localOutputDir, topics);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
