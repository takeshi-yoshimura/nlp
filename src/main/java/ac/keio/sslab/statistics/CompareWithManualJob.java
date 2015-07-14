package ac.keio.sslab.statistics;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.nlp.NLPConf;
import ac.keio.sslab.nlp.NLPJob;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;

public class CompareWithManualJob implements NLPJob {

	NLPConf conf = NLPConf.getInstance();
	
	@Override
	public String getJobName() {
		return "compareWithManual";
	}

	@Override
	public String getJobDescription() {
		return "estimates the similarity of LDA and manual classifications";
	}

	@Override
	public Options getOptions() {
		Options opt = new Options();
		opt.addOption("m", "manualResult", true, "csv File {commits, tag1,tag2,....}");
		opt.addOption("l", "ldaID", true, "ID for lda job");
		return opt;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("l") || !args.containsKey("m")) {
			System.err.println("Need to specify --ldaID and --manualResult");
			return;
		}

		File manualFile = new File(args.get("m"));
		LDAHDFSFiles hdfs = new LDAHDFSFiles(new Path(conf.ldaPath, args.get("l")));
		Configuration hdfsConf = new Configuration();
		
		System.out.println("Load manual classification from " + manualFile.getAbsolutePath());
		NamedMatrix docTags = NamedMatrix.buildFromCSV(manualFile, "doc", "tag");
		docTags = docTags.normalizeRow(); // calculate p(tag|doc) = 1 / N(tag|doc) for each doc as a Matrix row
		System.out.println("Built Matrix for " + docTags.rowSize() + " documents X " + docTags.colSize() + " tags");

		System.out.println("Load LDA classification from " + hdfs.cvbPath);
		NamedMatrix docTopics = NamedMatrix.buildFromLDAFiles(hdfs, hdfsConf, "doc", "topic");
		System.out.println("Built Matrix for " + docTopics.rowSize() + " documents X " + docTopics.colSize() + " topics");

		if (docTags.rowSize() != docTopics.rowSize()) {
			TreeMap<Integer, String> ignored = docTags.lostRowIndex(docTopics);
			for (String doc: ignored.values()) {
				System.out.println("document " + doc + " in manual classification was ignored in LDA classification.");
			}
			System.out.println("This can happen the message for a document is empty or a single paragraph with Signed-off-by or when all the words are stop-words. Compare anyway.");
			docTags = docTags.dropRows(ignored.keySet());
		}

		// now, calculate p(tag & topic)
		NamedMatrix tagTopics = docTags.transpose().times(docTopics);
		
		// build tagTopic ordered by the similarity and entropy
		ColNamedMatrix sortedTagTopics = tagTopics.buildColSorted().colSortedByValue().rowSortedByEntropy();

		File outDir = new File(conf.finalOutputFile, "compareWithManul");
		if (outDir.exists()) {
			try {
				FileUtils.delete(outDir, FileUtils.RECURSIVE);
			} catch (Exception e) {
				System.err.println("failed to delete " + outDir.getAbsolutePath());
				return;
			}
		}
		File tmpOutputFile = new File(NLPConf.tmpDirName, args.get("l"));
		tmpOutputFile.deleteOnExit();

		// write p(tag & topic) Matrix
		File tagTopicFile = new File(tmpOutputFile, "tagTopics.csv");
		try {
			tagTopics.dumpCSV(tagTopicFile);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to write " + tagTopicFile.getAbsolutePath());
			return;
		}

		// write topics ordered by similarity for each tag with two rows: ordered topic name and similarity
		File similarTopicFile = new File(tmpOutputFile, "similarTopics.csv");
		try {
			sortedTagTopics.dumpCSV(similarTopicFile, true);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed to write " + similarTopicFile.getAbsolutePath());
			return;
		}
		outDir.mkdirs();
		tmpOutputFile.renameTo(new File(outDir, tmpOutputFile.getName()));
	}

	@Override
	public void takeSnapshot() {
	}

	@Override
	public void restoreSnapshot() {
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
