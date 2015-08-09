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
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

//Read "maxTerms" words for each topic from HDFS
public class TopicReader {

	Map<Integer, List<Integer>> topicIDTermID;
	Map<Integer, String> termIDTermString;

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
		Comparator<Entry<Integer, Double>> reverser = new Comparator<Entry<Integer, Double>>() {
			public int compare(Entry<Integer, Double> e1, Entry<Integer, Double> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		};
		while (topicTermReader.seekNext()) {
			int topicID = topicTermReader.key();
			Vector vector = topicTermReader.val();
			Map<Integer, Double> termID = new HashMap<Integer, Double>();
			for (Element e: vector.all()) {
				termID.put(e.index(), e.get());
			}
			List<Entry<Integer, Double>> sortedTermID = new ArrayList<Entry<Integer, Double>>(termID.entrySet());
			Collections.sort(sortedTermID, reverser);
			List<Integer> topTermID = new ArrayList<Integer>();
			for (int i = 0; i < maxTerms && i < sortedTermID.size(); i++) {
				topTermID.add(sortedTermID.get(i).getKey());
			}
			topicIDTermID.put(topicID, topTermID);
		}
		topicTermReader.close();
	}
}