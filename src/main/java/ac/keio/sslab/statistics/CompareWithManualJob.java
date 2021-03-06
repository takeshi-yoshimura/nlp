package ac.keio.sslab.statistics;

import java.io.File;
import java.util.TreeMap;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.nlp.job.JobManager;
import ac.keio.sslab.nlp.job.LDAJob;
import ac.keio.sslab.nlp.job.NLPConf;
import ac.keio.sslab.nlp.job.NLPJobGroup;
import ac.keio.sslab.nlp.job.SingletonGroupNLPJob;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.utils.ColNamedMatrix;
import ac.keio.sslab.utils.NamedMatrix;

public class CompareWithManualJob extends SingletonGroupNLPJob {

	@Override
	public String getJobName() {
		return "lda.comp.man";
	}

	@Override
	public String getShortJobName() {
		return "cwm";
	}

	@Override
	public String getJobDescription() {
		return "estimates the similarity of LDA and manual classifications";
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
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("m", "manualResult", true, "csv File {commits, tag1,tag2,....}"));
		g.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(g);
		return opt;
	}

	@Override
	public void run(JobManager mgr, JobManager pMgr) throws Exception {
		NLPConf conf = NLPConf.getInstance();
		File manualFile = new File(mgr.getArgStr("m"));
		LDAHDFSFiles hdfs = new LDAHDFSFiles(pMgr.getHDFSOutputDir());
		
		System.out.println("Load manual classification from " + manualFile.getAbsolutePath());
		NamedMatrix docTags = NamedMatrix.buildFromCSV(manualFile, "doc", "tag").normalizeRow(); // calculate p(tag|doc) = 1 / N(tag|doc) for each doc as a Matrix row
		System.out.println("Built Matrix for " + docTags.rowSize() + " documents X " + docTags.colSize() + " tags");

		System.out.println("Load LDA classification from " + hdfs.cvbPath);
		NamedMatrix docTopics = NamedMatrix.buildFromLDAFiles(hdfs, conf.hdfs, "doc", "topic");
		System.out.println("Built Matrix for " + docTopics.rowSize() + " documents X " + docTopics.colSize() + " topics");

		if (docTags.rowSize() != docTopics.rowSize()) {
			TreeMap<Integer, String> ignored = docTags.lostRowIndex(docTopics);
			for (String doc: ignored.values()) {
				System.out.println("document " + doc + " in manual classification was ignored in LDA classification.");
			}
			System.out.println("This can happen the message for a document is empty or a single paragraph with Signed-off-by or when all the words are stop-words. Compare anyway.");
			docTags = docTags.dropRows(ignored.keySet());
		}

		NamedMatrix tagAndTopics = docTags.transpose().times(docTopics).divide(docTags.rowSize()); // calculate p(tag & topic) = sum_doc{p(tag|doc)p(topic|doc)p(doc)}
		NamedMatrix tagTags = docTags.transpose().times(NamedMatrix.buildOne(docTags.rowSize(), docTopics.colSize())).divide(docTags.rowSize());
		NamedMatrix tagTopics = docTopics.transpose().times(NamedMatrix.buildOne(docTopics.rowSize(), docTags.colSize())).divide(docTopics.rowSize()).transpose();
		NamedMatrix tagTagPlusTopics = tagTags.plus(tagTopics);
		tagTagPlusTopics.setRowGroupName("tag");
		tagTagPlusTopics.setColGroupName("topic");
		NamedMatrix harmony = tagAndTopics.times(2).divideOneByOne(tagTagPlusTopics); // 2 * p(tag & topic) / (p(tag) + p(topic))
		ColNamedMatrix sortedHarmony = harmony.buildColSorted().colSortedByValue();

		/**
		 * It was not so a good strategy to calculate p(topic|tag) and p(tag|topic) for using the similarity of two classifications.
		 * This is because classifications often cannot avoid too general classes among documents.
		 * For example, LDA should generate topics for 'and' 'then' etc. in any corpus and even manual classification determines code cleanups and other major categories.
		 * Comparing them results in recognizing too common classes in each other's likely-hood inference,
		 * although important classes are often rare among documents. Thus, this way guides us only meaningless results.
		 * The idea of using similar concept of IDF potentially increases the complexity for validating results.
		 * 
		 * // build p(topic|tag)
		 * // p(topic|tag) = p(tag, topic) / p(tag) = sum_doc{p(topic|doc)p(tag|doc)p(doc)} / sum_doc{p(tag|doc)p(doc)}
		 * //              = sum_doc{p(topic|doc)* p(tag|doc) / sum_doc{p(tag|doc)}} = sum_doc{p(topic|doc) * p(doc|tag)}
		 * NamedMatrix tagTopics = docTags.transpose().normalizeRow().times(docTopics);
		 * ColNamedMatrix sortedTagTopics = tagTopics.buildColSorted().colSortedByValue().rowSortedByEntropy();
		 * 
		 * // build p(tag|topic)
		 * NamedMatrix topicTags = docTopics.transpose().normalizeRow().times(docTags);
		 * ColNamedMatrix sortedTopicTags = topicTags.buildColSorted().colSortedByValue().rowSortedByEntropy();
		 */

		File outDir = new File(conf.finalOutputFile, "compareWithManul/" + mgr.getArgStr("l"));
		if (outDir.exists()) {
			try {
				FileUtils.delete(outDir, FileUtils.RECURSIVE);
			} catch (Exception e) {
				System.err.println("failed to delete " + outDir.getAbsolutePath());
				return;
			}
		}
		File tmpOutputFile = new File(NLPConf.tmpDirName, "compareWithManual/" + mgr.getArgStr("l"));
		tmpOutputFile.mkdirs();
		tmpOutputFile.deleteOnExit();

		File tagTopicMatrixFile = new File(tmpOutputFile, "tagTopicMatrix.csv");
		File tagTopicKVSFile = new File(tmpOutputFile, "tagTopicKVS.csv");
		File tagHarmonyMatrixFile = new File(tmpOutputFile, "tagTopicKVS.csv");
		File tagHarmonyKVSFile = new File(tmpOutputFile, "tagTopicKVS.csv");

		// write p(tag & topic) Matrix
		tagAndTopics.dumpCSVInMatrixFormat(tagTopicMatrixFile);
		tagAndTopics.dumpCSVInKeyValueFormat(tagTopicKVSFile);

		// write topics ordered by similarity for each tag with two rows: ordered topic name and similarity
		sortedHarmony.dumpCSV(tagHarmonyMatrixFile, false);
		sortedHarmony.dumpCSV(tagHarmonyKVSFile, false);
		outDir.mkdirs();
		tmpOutputFile.renameTo(outDir);
	}

	@Override
	public boolean runInBackground() {
		return false;
	}
}
