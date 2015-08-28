package ac.keio.sslab.utils.mahout;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;

import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;

public class SimpleLDAReader {

	public static Map<Integer, String> getTopicTerm(FileSystem fs, Path dictionaryPath, Path topicTermPath) throws IOException {
		Map<Integer, String> topics = new HashMap<Integer, String>();
		for (Entry<Integer, List<String>> e: new LDATopicReader(dictionaryPath, topicTermPath, fs, 2).getTopics().entrySet()) {
			topics.put(e.getKey(), "T" + e.getKey() + "-" + e.getValue().get(0) + "-" + e.getValue().get(1));
		}
		return topics;
	}

	public static Map<String, List<Integer>> getDocTopic(FileSystem fs, Path docIndexPath, Path docTopicPath) throws IOException {
		return new LDADocTopicReader(docIndexPath, docTopicPath, fs, 10).getDocuments();
	}

	public static Map<Integer, String> getDocIndex(FileSystem fs, Path docIndexPath) throws IOException {
		Map<Integer, String> docIndex = new HashMap<Integer, String>();
		SequenceDirectoryReader<Integer, String> dictionaryReader = new SequenceDirectoryReader<>(docIndexPath, fs, Integer.class, String.class);
		while (dictionaryReader.seekNext()) {
			int documentId = dictionaryReader.key();
			String documentName = dictionaryReader.val();
			docIndex.put(documentId, documentName);
		}
		dictionaryReader.close();
		return docIndex;
	}

	public static Map<String, Vector> getDocTopicVector(FileSystem fs, Path docIndexPath, Path docTopicPath) throws IOException {
		Map<String, Vector> docTopic = new HashMap<String, Vector>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(docTopicPath, fs, Integer.class, Vector.class);
		Map<Integer, String> docIndex = getDocIndex(fs, docIndexPath);
		while (reader.seekNext()) {
			docTopic.put(docIndex.get(reader.key()), reader.val());
		}
		reader.close();
		return docTopic;
	}
}
