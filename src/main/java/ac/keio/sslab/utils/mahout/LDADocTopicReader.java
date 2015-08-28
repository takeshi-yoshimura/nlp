package ac.keio.sslab.utils.mahout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;

public class LDADocTopicReader {
	Map<Integer, String> docIndex;
	Map<Integer, List<Integer>> docTopicId;

	public LDADocTopicReader(Path docIndex, Path documentDir, FileSystem fs, int maxTopics) throws IOException {
		loadDocumentIndex(docIndex, fs);
		loadDocumentDir(documentDir, fs, maxTopics);
	}

	public Map<String, List<Integer>> getDocuments() {
		Map<String, List<Integer>> ret = new HashMap<String, List<Integer>>();
		for (Entry<Integer, String> topic: docIndex.entrySet()) {
			ret.put(topic.getValue(), docTopicId.get(topic.getKey()));
		}
		return ret;
	}

	public void loadDocumentIndex(Path docIndexPath, FileSystem fs) throws IOException {
		docIndex = new HashMap<Integer, String>();
		SequenceDirectoryReader<Integer, String> dictionaryReader = new SequenceDirectoryReader<>(docIndexPath, fs, Integer.class, String.class);
		while (dictionaryReader.seekNext()) {
			int documentId = dictionaryReader.key();
			String documentName = dictionaryReader.val();
			docIndex.put(documentId, documentName);
		}
		dictionaryReader.close();
	}

	public void loadDocumentDir(Path documentDir, FileSystem fs, int maxTopics) throws IOException {
		docTopicId = new HashMap<Integer, List<Integer>>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(documentDir, fs, Integer.class, Vector.class);

		Comparator<Entry<Integer, Double>> reverser = new Comparator<Entry<Integer, Double>>() {
			public int compare(Entry<Integer, Double> e1, Entry<Integer, Double> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		};
		while (reader.seekNext()) {
			int docId = reader.key();
			Vector vector = reader.val();
			Map<Integer, Double> docTopic = new HashMap<Integer, Double>();
			for (Element e: vector.all()) {
				docTopic.put(e.index(), e.get());
			}
			List<Entry<Integer, Double>> sortedDocTopic = new ArrayList<Entry<Integer, Double>>(docTopic.entrySet());
			Collections.sort(sortedDocTopic, reverser);
			List<Integer> topicId = new ArrayList<Integer>();
			for (int i = 0; i < maxTopics && i < sortedDocTopic.size(); i++) {
				topicId.add(sortedDocTopic.get(i).getKey());
			}
			docTopicId.put(docId, topicId);
		}
		reader.close();
	}
}
