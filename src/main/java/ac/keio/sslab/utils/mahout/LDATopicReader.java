package ac.keio.sslab.utils.mahout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ac.keio.sslab.utils.SimpleSorter;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;

//Read "maxTerms" words for each topic from HDFS
public class LDATopicReader {

	Map<Integer, List<Integer>> topicIDTermID;
	Map<Integer, List<Double>> topicIDTermProbs;
	Map<Integer, String> termIDTermString;

	public LDATopicReader() {
	}

	public LDATopicReader(Path dictionary, Path topicTerm, FileSystem fs, int maxTerms) throws IOException {
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

	public Map<String, Double> getTopicTermProbs(int topicID) {
		Map<String, Double> ret = new HashMap<>();
		for (int i = 0; i < topicIDTermProbs.get(topicID).size(); i++) {
			ret.put(termIDTermString.get(topicIDTermID.get(topicID).get(i)), topicIDTermProbs.get(topicID).get(i));
		}
		return ret;
	}

	public int numTopics() {
		return topicIDTermID.size();
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
		topicIDTermProbs = new HashMap<>();
		SequenceDirectoryReader<Integer, Vector> topicTermReader = new SequenceDirectoryReader<>(topicTerm, fs, Integer.class, Vector.class);
		while (topicTermReader.seekNext()) {
			int topicID = topicTermReader.key();
			Vector vector = topicTermReader.val();
			Map<Integer, Double> termID = new HashMap<Integer, Double>();
			for (Element e: vector.all()) {
				termID.put(e.index(), e.get());
			}
			List<Entry<Integer, Double>> sortedTermID = SimpleSorter.reverse(termID);
			List<Integer> topTermID = new ArrayList<Integer>();
			List<Double> topTermProbs = new ArrayList<>();
			for (int i = 0; i < maxTerms && i < sortedTermID.size(); i++) {
				topTermID.add(sortedTermID.get(i).getKey());
				topTermProbs.add(sortedTermID.get(i).getValue());
			}
			topicIDTermID.put(topicID, topTermID);
			topicIDTermProbs.put(topicID, topTermProbs);
		}
		topicTermReader.close();
	}
}