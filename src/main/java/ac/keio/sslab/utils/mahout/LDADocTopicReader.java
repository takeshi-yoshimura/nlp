package ac.keio.sslab.utils.mahout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import ac.keio.sslab.utils.SimpleSorter;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;

public class LDADocTopicReader {
	Map<Integer, String> docIndex;
	Map<Integer, List<Integer>> docTopicId;
	Map<Integer, List<Double>> docTopicProbs;

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

	public Map<Integer, Double> getDocumentProbs(int docId) {
		Map<Integer, Double> ret = new HashMap<>();
		for (int i = 0; i < docTopicId.get(docId).size(); i++) {
			ret.put(docTopicId.get(docId).get(i), docTopicProbs.get(docId).get(i));
		}
		return ret;
	}

	public int numDocs() {
		return docTopicId.size();
	}

	public String docName(int docId) {
		return docIndex.get(docId);
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
		docTopicProbs = new HashMap<>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(documentDir, fs, Integer.class, Vector.class);
		while (reader.seekNext()) {
			int docId = reader.key();
			Vector vector = reader.val();
			Map<Integer, Double> docTopic = new HashMap<Integer, Double>();
			for (Element e: vector.all()) {
				docTopic.put(e.index(), e.get());
			}
			List<Entry<Integer, Double>> sortedDocTopic = SimpleSorter.reverse(docTopic);
			List<Integer> topicId = new ArrayList<Integer>();
			List<Double> topicProbs = new ArrayList<>();
			for (int i = 0; i < maxTopics && i < sortedDocTopic.size(); i++) {
				topicId.add(sortedDocTopic.get(i).getKey());
				topicProbs.add(sortedDocTopic.get(i).getValue());
			}
			docTopicId.put(docId, topicId);
			docTopicProbs.put(docId, topicProbs);
		}
		reader.close();
	}
}
