package ac.keio.sslab.nlp;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import ac.keio.sslab.nlp.lda.DocumentReader;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class LDADumpJob implements NLPJob {

	@Override
	public String getJobName() {
		return "ldaDump";
	}

	@Override
	public String getJobDescription() {
		return "dump lda results";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("l", "ldaID", true, "ID of lda job"));
		g.setRequired(true);

		Options opts = new Options();
		opts.addOptionGroup(g);
		return opts;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		File ldaDumpFile = new File(conf.finalOutputFile, "ldaDump");
		File outputFile = mgr.getLocalArgFile(ldaDumpFile, "l");
		if (!JobUtils.promptDeleteDirectory(outputFile, mgr.doForceWrite())) {
			return;
		}
		outputFile.mkdirs();

		LDAHDFSFiles hdfs = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		try {
			File topicFile = new File(outputFile, "topics.txt");
			System.out.println("Extracting top 10 topics: " + topicFile.getAbsolutePath());
			TopicReader topReader = new TopicReader(hdfs.dictionaryPath, hdfs.topicPath, conf.hdfs, 10);
			PrintWriter pw = JobUtils.getPrintWriter(topicFile);
			StringBuilder sb = new StringBuilder();
			for (Entry<Integer, List<String>> topic: topReader.getTopics().entrySet()) {
				sb.setLength(0);
				for (String word: topic.getValue()) {
					if (sb.length() > 0) {
						sb.append(' ');
					}
					sb.append(word);
				}
				pw.println(topic.getKey() + "\t" + sb.toString());
			}
			pw.close();

			File documentFile = new File(outputFile, "documents.txt");
			System.out.println("Extracting documents with top 10 topics: " + documentFile.getAbsolutePath());
			PrintWriter pw2 = JobUtils.getPrintWriter(documentFile);
			DocumentReader docReader = new DocumentReader(hdfs.docIndexPath, hdfs.documentPath, conf.hdfs, 10);
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
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Retriving topics and documents failed: " + e.toString());
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
