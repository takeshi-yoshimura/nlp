package ac.keio.sslab.analytics;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ac.keio.sslab.nlp.corpus.IdIndexReader;

public class PatchIDResolver {

	public static Map<Integer, List<String>> getPointIDtoPatchIDs(File corpusDir, File bottomupDir) throws IOException {
		Map<Integer, Integer> corpusIDMap = new CorpusIDIndexReader(bottomupDir).revAll();

		Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
		IdIndexReader idIndex = new IdIndexReader(corpusDir);
		while (idIndex.seekNext()) {
			ret.put(corpusIDMap.get(idIndex.key()), idIndex.val());
		}
		idIndex.close();

		return ret;
	}

	public static int toPointID(String patchID, File corpusDir, File bottomupDir) throws IOException {
		int corpusIndex = -1;
		IdIndexReader idIndex = new IdIndexReader(corpusDir);
		while (idIndex.seekNext()) {
			if (!idIndex.val().contains(patchID)) {
				corpusIndex = idIndex.key();
				break;
			}
		}
		idIndex.close();
		if (corpusIndex == -1) {
			throw new IOException("Could not find ID " + patchID);
		}

		int pointID = -1;
		CorpusIDIndexReader corpusID = new CorpusIDIndexReader(bottomupDir);
		while (corpusID.seekNext()) {
			if (corpusID.val() == corpusIndex) {
				pointID = corpusID.key();
				break;
			}
		}
		corpusID.close();
		if (pointID == -1) {
			throw new IOException("Could not find ID " + corpusIndex + " (pulled with " + patchID + ")");
		}

		return pointID;
	}

}
