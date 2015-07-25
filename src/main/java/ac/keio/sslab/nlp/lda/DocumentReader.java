package ac.keio.sslab.nlp.lda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;

public class DocumentReader {
	Map<Integer, String> docIndex;
	Map<Integer, List<Integer>> docTopicId;

	class FirstReverseSorter implements Comparator<Pair<Double, ?>> {
		@Override
		public int compare(Pair<Double, ?> p1, Pair<Double, ?> p2) {
			double d1 = p1.getFirst();
			double d2 = p2.getFirst();
			if (d1 > d2) {
				return -1;
			} else if (d1 == d2) {
				return 0;
			}
			return 1;
		}
	}

	public DocumentReader(Path docIndex, Path documentDir, Configuration conf, int maxTopics) throws IOException {
		loadDocumentIndex(docIndex, conf);
		loadDocumentDir(documentDir, conf, maxTopics);
	}

	public Map<String, List<Integer>> getDocuments() {
		Map<String, List<Integer>> ret = new HashMap<String, List<Integer>>();
		for (Entry<Integer, String> topic: docIndex.entrySet()) {
			ret.put(topic.getValue(), docTopicId.get(topic.getKey()));
		}
		return ret;
	}

	public void loadDocumentIndex(Path docIndexPath, Configuration conf) throws IOException {
		docIndex = new HashMap<Integer, String>();
		SequenceDirectoryReader<Integer, String> dictionaryReader = new SequenceDirectoryReader<>(docIndexPath, conf);
		while (dictionaryReader.seekNext()) {
			int documentId = dictionaryReader.key();
			String documentName = dictionaryReader.val();
			docIndex.put(documentId, documentName);
		}
		dictionaryReader.close();
	}

	public void loadDocumentDir(Path documentDir, Configuration conf, int maxTopics) throws IOException {
		docTopicId = new HashMap<Integer, List<Integer>>();
		SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(documentDir, conf);
		FirstReverseSorter sorter = new FirstReverseSorter();
		while (reader.seekNext()) {
			int docId = reader.key();
			Vector vector = reader.val();
			List<Pair<Double, Integer>> docTopic = new ArrayList<Pair<Double, Integer>>();
			for (Element e: vector.all()) {
				docTopic.add(new Pair<Double, Integer>(e.get(), e.index()));
			}
			Collections.sort(docTopic, sorter);
			List<Integer> topicId = new ArrayList<Integer>();
			for (int i = 0; i < maxTopics && i < docTopic.size(); i++) {
				topicId.add(docTopic.get(i).getSecond());
			}
			docTopicId.put(docId, topicId);
		}
		reader.close();
	}
}
