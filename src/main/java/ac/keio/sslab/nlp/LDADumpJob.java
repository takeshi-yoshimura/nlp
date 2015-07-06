package ac.keio.sslab.nlp;

import java.io.File;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.nlp.lda.DocumentReader;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

public class LDADumpJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();

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
		Options opts = new Options();
		opts.addOption("l", "ldaID", true, "ID of lda job");
		return opts;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("l")) {
			System.err.println("Need to specify --ldaID");
			return;
		}
		File ldaDumpFile = new File(conf.finalOutputFile, "ldaDump");
		File outputFile = new File(ldaDumpFile, args.get("l"));
		if (args.containsKey("ow")) {
			try {
				FileUtils.delete(outputFile, FileUtils.RECURSIVE);
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Deleting existing directory " + outputFile.getAbsolutePath() + " failed: " + e.toString());
				return;
			}
		}
		outputFile.mkdirs();

		LDAHDFSFiles hdfs = new LDAHDFSFiles(new Path(conf.ldaPath, args.get("l")));
		Configuration conf = new Configuration();
		try {
			File topicFile = new File(outputFile, "topics.txt");
			System.out.println("Extracting top 10 topics: " + topicFile.getAbsolutePath());
			TopicReader topReader = new TopicReader(hdfs.dictionaryPath, hdfs.topicPath, conf, 10);
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
			DocumentReader docReader = new DocumentReader(hdfs.docIndexPath, hdfs.documentPath, conf, 10);
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
	public void takeSnapshot() {
		/* do nothing */
	}

	@Override
	public void restoreSnapshot() {
		/* do nothing */
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
