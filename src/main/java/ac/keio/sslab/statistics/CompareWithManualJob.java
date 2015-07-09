package ac.keio.sslab.statistics;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.MatrixSlice;
import org.apache.mahout.math.Vector.Element;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.nlp.JobUtils;
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
		DocumentClassMatrix docTags = new DocumentTagMatrix(manualFile);
		System.out.println("Built Matrix for " + docTags.getDocIndex().size() + " documents X " + docTags.getClassIndex().size() + " tags");

		System.out.println("Load LDA classification from " + hdfs.cvbPath);
		DocumentClassMatrix docTopics = new DocumentTopicMatrix(hdfs, hdfsConf);
		System.out.println("Built Matrix for " + docTopics.getDocIndex().size() + " documents X " + docTopics.getClassIndex().size() + " topics");

		if (docTags.getDocIndex().size() != docTopics.getDocIndex().size()) {
			Set<String> ignored = new HashSet<String>(docTags.getDocIndex().values());
			ignored.removeAll(docTopics.getDocIndex().values());
			for (String doc: ignored) {
				System.out.println("document " + doc + " in manual classification was ignored in LDA classification.");
			}
			System.out.println("This can happen the message for a document is empty or a single paragraph with Signed-off-by or when all the words are stop-words. Compare anyway.");

			Set<Integer> drop = new HashSet<Integer>();
			for (Entry<Integer, String> e: docTags.getDocIndex().entrySet()) {
				if (ignored.contains(e.getValue())) {
					drop.add(e.getKey());
				}
			}
			docTags = docTags.dropRows(drop);
		}

		// now, calculate p(tag & topic)
		Matrix tagTopics = docTags.transpose().times(docTopics);
		
		// build tagTopic ordered by the similarity and entropy
		MatrixColInRawSorter matrixSorter = new MatrixColInRawSorter(tagTopics);

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
			PrintWriter pw = JobUtils.getPrintWriter(tagTopicFile);
			// writes CSV header
			StringBuilder sb = new StringBuilder("#tag");
			for (Entry<Integer, String> tag: docTags.getClassIndex().entrySet()) {
				sb.append(',').append(tag.getValue());
			}
			pw.println(sb.toString());
			
			// write each p(topic|tag) for each tag in a raw
			for (MatrixSlice tagTopic: tagTopics) {
				sb.setLength(0);
				sb.append(docTags.getClassIndex().get(tagTopic.index()));
				for (Element p: tagTopic.all()) {
					sb.append(',').append(p.get());
				}
				pw.println(sb.toString());
			}
			pw.close();
		} catch (Exception e) {
			System.err.println("Failed to write " + tagTopicFile.getAbsolutePath());
			return;
		}
		
		// write topics ordered by similarity for each tag with two rows: ordered topic name and similarity. tags are ordered by the entropy of the similarity
		File similarTopicFile = new File(tmpOutputFile, "similarTopics.csv");
		try {
			PrintWriter pw = JobUtils.getPrintWriter(similarTopicFile);
			StringBuilder sb = new StringBuilder();
			Matrix valueMatrix = matrixSorter.getValueMatrix();
			Matrix colOrderMatrix = matrixSorter.getColOrderMatrix();
			Map<Integer, Integer> rawOrderMap = matrixSorter.getRawOrderMap();
			Map<Integer, Double> rawEntropyMap = matrixSorter.getRawEntropyMap();
			
			for (MatrixSlice values: valueMatrix) {
				String tag = docTags.getClassIndex().get(rawOrderMap.get(values.index()));
				sb.append("Tag,").append(tag);
				sb.append("\nEntropy,").append(rawEntropyMap.get(values.index()));

				sb.append("\nTopic");
				for (int i = 0; i < colOrderMatrix.columnSize(); i++) {
					sb.append(',').append(docTopics.getClassIndex().get(colOrderMatrix.get(values.index(), i)));
				}
				sb.append("\nSimilarity");
				for (Element e: values.all()) {
					sb.append(',').append(e.get());
				}
				pw.println(sb.append('\n').toString());
				sb.setLength(0);
			}
			pw.close();
		} catch (Exception e) {
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
