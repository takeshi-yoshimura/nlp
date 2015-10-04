package ac.keio.sslab.nlp.job;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.utils.SimpleSorter;
import ac.keio.sslab.utils.mahout.LDADocTopicReader;
import ac.keio.sslab.utils.mahout.LDATopicReader;

public class LDADumpJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "lda.dump";
	}

	@Override
	public String getShortJobName() {
		return "ld";
	}

	@Override
	public NLPJobGroup getParentJobGroup() {
		return new LDAJob();
	}

	@Override
	public File getLocalJobDir() {
		return new File(NLPConf.getInstance().finalOutputFile, getJobName());
	}

	@Override
	public Path getHDFSJobDir() {
		return null;
	}

	@Override
	public String getJobDescription() {
		return "dump lda results";
	}

	@Override
	public Options getOptions() {
		return null;
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		NLPConf conf = NLPConf.getInstance();
		File outputFile = mgr.getLocalOutputDir();
		if (!JobUtils.promptDeleteDirectory(outputFile, mgr.doForceWrite())) {
			return;
		}
		outputFile.mkdirs();

		LDAHDFSFiles hdfs = new LDAHDFSFiles(pMgr.getHDFSOutputDir());
		File topicFile = new File(outputFile, "topics.txt");
		File probFile = new File(outputFile, "topicProbs.txt");
		System.out.println("Extracting top 10 topics: " + topicFile.getAbsolutePath());
		LDATopicReader topReader = new LDATopicReader(hdfs.dictionaryPath, hdfs.topicPath, conf.hdfs, 10);
		PrintWriter pw = JobUtils.getPrintWriter(topicFile);
		PrintWriter pw3 = JobUtils.getPrintWriter(probFile);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < topReader.numTopics(); i++) {
			sb.setLength(0);
			pw3.println("# topic No." + i);
			for (Entry<String, Double> topic: SimpleSorter.reverse(topReader.getTopicTermProbs(i))) {
				if (sb.length() > 0) {
					sb.append(' ');
				}
				sb.append(topic.getKey());
				pw3.println(topic.getKey() + "," + topic.getValue());
			}
			pw.println(i + "\t" + sb.toString());
		}
		pw.close();
		pw3.close();

		File documentFile = new File(outputFile, "documents.txt");
		System.out.println("Extracting documents with top 10 topics: " + documentFile.getAbsolutePath());
		PrintWriter pw2 = JobUtils.getPrintWriter(documentFile);
		LDADocTopicReader docReader = new LDADocTopicReader(hdfs.docIndexPath, hdfs.documentPath, conf.hdfs, 10);
		for (Entry<String, List<Integer>> document: docReader.getDocuments().entrySet()) {
			sb.setLength(0);
			for (int topicId: document.getValue()) {
				if (sb.length() > 0) {
					sb.append(' ');
				}
				sb.append(topicId);
			}
			pw2.println(document.getKey() + "\t" + sb.toString());
		}
		pw2.close();
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
