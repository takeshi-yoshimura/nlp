package ac.keio.sslab.statistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.mahout.math.SparseMatrix;

// build Matrix from CSV with lines: "docName,tag1,tag2,..."
// weights of all the tags are same. we do no permit duplicated tags
public class DocumentTagMatrix extends DocumentClassMatrix {
	
	public DocumentTagMatrix(File csvFile) {
		super(null, new TreeMap<Integer, String>(), new TreeMap<Integer, String>());
		readFromCSV(csvFile);
	}

	protected void readFromCSV(File inputFile) {
		Map<Integer, List<Integer>> tmpDocTags = new HashMap<Integer, List<Integer>>();

		try {
			BufferedReader br = new BufferedReader(new FileReader(inputFile));
			String line;
			Map<String, Integer> revTagNames = new HashMap<String, Integer>();

			// split a single record into {docIndex, doc}, {tagIndex, tag}
			while ((line = br.readLine()) != null) {
				if (line.isEmpty())
					continue;
				String[] splitLine = line.split(",");
				List<Integer> tagIndices = new ArrayList<Integer>();
				for (int i = 1; i < splitLine.length; i++) {
					if (!revTagNames.containsKey(splitLine[i])) {
						classIndex.put(classIndex.size(), splitLine[i]);
					}
					tagIndices.add(revTagNames.get(splitLine[i]));
				}
				tmpDocTags.put(docIndex.size(), tagIndices);
				docIndex.put(docIndex.size(), splitLine[0]);
			}
			br.close();
		} catch (Exception e) {
			System.err.println("Failed to load Local file " + inputFile.getAbsolutePath() + ": " + e.getMessage());
			docIndex = null;
			classIndex = null;
			return;
		}

		// calculate p(tag|doc) = 1 / N(tag|doc) for each doc as a Matrix row
		docClass = new SparseMatrix(tmpDocTags.size(), classIndex.size());
		for (Entry<Integer, List<Integer>> e: tmpDocTags.entrySet()) {
			for (int index: e.getValue()) {
				docClass.set(e.getKey(), index, 1.0 / e.getValue().size());;
			}
		}
	}
}
