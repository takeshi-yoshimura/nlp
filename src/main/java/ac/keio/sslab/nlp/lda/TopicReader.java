package ac.keio.sslab.nlp.lda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

//Read "maxTerms" words for each topic from HDFS
public class TopicReader {

	Map<Integer, List<Integer>> topicIDTermID;
	Map<Integer, String> termIDTermString;

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

	public TopicReader() {
	}

	public TopicReader(Path dictionary, Path topicTerm, FileSystem fs, int maxTerms) throws IOException {
		loadDictionary(dictionary, fs);
		loadTopicTermDir(topicTerm, fs, maxTerms);
	}

	public Map<Integer, List<String>> getTopics() {
		Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
		for (Entry<Integer, List<Integer>> topic: topicIDTermID.entrySet()) {
			List<String> words = new ArrayList<String>();
			for (int termID: topic.getValue()) {
				words.add(termIDTermString.get(termID));
			}
			ret.put(topic.getKey(), words);
		}
		return ret;
	}

	public void loadDictionary(Map<Integer, String> dictionary) {
		termIDTermString = dictionary;
	}

	public void loadDictionary(Path dictionary, FileSystem fs) throws IOException {
		termIDTermString = new HashMap<Integer, String>();
		SequenceDirectoryReader<String, Integer> dictionaryReader = new SequenceDirectoryReader<>(dictionary, fs, String.class, Integer.class);
		while (dictionaryReader.seekNext()) {
			int termID = dictionaryReader.val();
			String term = dictionaryReader.key();
			termIDTermString.put(termID, term);
		}
		dictionaryReader.close();
	}

	public void loadTopicTermDir(Path topicTerm, FileSystem fs, int maxTerms) throws IOException {
		topicIDTermID = new HashMap<Integer, List<Integer>>();
		SequenceDirectoryReader<Integer, Vector> topicTermReader = new SequenceDirectoryReader<>(topicTerm, fs, Integer.class, Vector.class);
		FirstReverseSorter sorter = new FirstReverseSorter();
		while (topicTermReader.seekNext()) {
			int topicID = topicTermReader.key();
			Vector vector = topicTermReader.val();
			List<Pair<Double, Integer>> termID = new ArrayList<Pair<Double, Integer>>();
			for (Element e: vector.all()) {
				termID.add(new Pair<Double, Integer>(e.get(), e.index()));
			}
			Collections.sort(termID, sorter);
			List<Integer> topTermID = new ArrayList<Integer>();
			for (int i = 0; i < maxTerms && i < termID.size(); i++) {
				topTermID.add(termID.get(i).getSecond());
			}
			topicIDTermID.put(topicID, topTermID);
		}
		topicTermReader.close();
	}
}