package ac.keio.sslab.statistics;

import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;
import ac.keio.sslab.nlp.lda.TopicReader;

// build Matrix from LDA results
public class DocumentTopicMatrix extends DocumentClassMatrix {

	public DocumentTopicMatrix(LDAHDFSFiles hdfs, Configuration hdfsConf) {
		super(null, new TreeMap<Integer, String>(), new TreeMap<Integer, String>());
		readFromLDAFiles(hdfs, hdfsConf);
	}

	protected void readFromLDAFiles(LDAHDFSFiles hdfs, Configuration hdfsConf) {
		try {
			for (Entry<Integer, List<String>> e: new TopicReader(hdfs.dictionaryPath, hdfs.topicPath, hdfsConf, 2).getTopics().entrySet()) {
				classIndex.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
			}
		} catch (Exception e) {
			System.err.println("Failed to load HDFS files " + hdfs.dictionaryPath + " or " + hdfs.topicPath + ": " + e.getMessage());
			classIndex = null;
			return;
		}

		try {
			SequenceDirectoryReader dictionaryReader = new SequenceDirectoryReader(hdfs.docIndexPath, hdfsConf);
			IntWritable key = new IntWritable();
			Text value = new Text();
			while (dictionaryReader.next(key, value)) {
				int documentId = key.get();
				String documentName = value.toString();
				docIndex.put(documentId, documentName);
			}
			dictionaryReader.close();
		} catch (Exception e) {
			System.err.println("Failed to load HDFS file " + hdfs.docIndexPath + ": " + e.getMessage());
			docIndex = null;
			classIndex = null;
			return;
		}

		try {
			SequenceDirectoryReader reader = new SequenceDirectoryReader(hdfs.documentPath, hdfsConf);
			IntWritable key = new IntWritable();
			VectorWritable value = new VectorWritable();
			while (reader.next(key, value)) {
				for (Element p: value.get().nonZeroes()) {
					docClass.set(key.get(), p.index(), p.get());
				}
			}
			reader.close();
		} catch (Exception e) {
			System.err.println("Failed to loead HDFS file " + hdfs.docIndexPath + " or " + hdfs.documentPath + ": " + e.getMessage());
			docClass = null;
			docIndex = null;
			classIndex = null;
		}
	}
}
