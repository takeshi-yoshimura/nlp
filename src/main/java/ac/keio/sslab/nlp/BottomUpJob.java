package ac.keio.sslab.nlp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.clustering.bottomup.BottomupClustering;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

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
		required.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(required);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		LDAHDFSFiles ldaFiles = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		File localOutputDir = new File(conf.finalOutputFile, "bottomup/" + mgr.getJobID());
		File clustersFile = new File(localOutputDir.getAbsolutePath(), "clusters.csv");

		try {
			BottomupClustering.run(ldaFiles.documentPath, conf.hdfs, clustersFile, mgr.doForceWrite(), getTopicStr(ldaFiles, conf));
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	public Map<Integer, String> getTopicStr(LDAHDFSFiles ldaFiles, NLPConf conf) throws IOException {
		Map<Integer, String> topics = new HashMap<Integer, String>();
		for (Entry<Integer, List<String>> e: new TopicReader(ldaFiles.dictionaryPath, ldaFiles.topicPath, conf.hdfs, 2).getTopics().entrySet()) {
			topics.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
		}
		return topics;
	}

	@Override
	public boolean runInBackground() {
		return true;
	}

}
