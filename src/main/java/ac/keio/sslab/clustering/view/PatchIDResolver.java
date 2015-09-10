package ac.keio.sslab.clustering.view;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PatchIDResolver {

	File idIndexFile, corpusIDIndexFile;

	public PatchIDResolver(File idIndexFile, File corpusIDIndexFile) {
		this.idIndexFile = idIndexFile;
		this.corpusIDIndexFile = corpusIDIndexFile;
	}

	public Map<Integer, List<String>> getPointIDtoPatchIDs() throws IOException {
		Map<String, Integer> corpusIDMap = new HashMap<String, Integer>();
		BufferedReader br = new BufferedReader(new FileReader(corpusIDIndexFile));
		String line = null;
		while((line = br.readLine()) != null) {
			String [] s = line.split(",");
			corpusIDMap.put(s[1], Integer.parseInt(s[0]));
		}
		br.close();

		Map<Integer, List<String>> ret = new HashMap<Integer, List<String>>();
		BufferedReader reader = new BufferedReader(new FileReader(idIndexFile));
		while ((line = reader.readLine()) != null) {
			String [] s = line.split("\t\t");
			String [] s2 = s[1].split(",");
			List<String> a = new ArrayList<String>();
			for (int i = 0; i < s2.length; i++) {
				if (s2[i].isEmpty()) {
					continue;
				}
				a.add(s2[i]);
			}
			ret.put(corpusIDMap.get(s[0]), a);
		}
		reader.close();

		return ret;
	}

	public int toPointID(String patchID) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(idIndexFile));
		String corpusIndex = null;
		String line = null;
		while ((line = br.readLine()) != null) {
			if (!line.contains(patchID)) {
				continue;
			}
			corpusIndex = line.split("\t")[0];
			break;
		}
		br.close();
		if (corpusIndex == null) {
			throw new IOException("Could not find ID " + patchID + " in " + idIndexFile);
		}

		int pointID = -1;
		br = new BufferedReader(new FileReader(corpusIDIndexFile));
		while ((line = br.readLine()) != null) {
			if (!line.startsWith(corpusIndex)) {
				continue;
			}
			pointID = Integer.parseInt(line.split(",")[1]);
			break;
		}
		if (pointID == -1) {
			throw new IOException("Could not find ID " + corpusIndex + " (pulled with " + patchID + " from " + idIndexFile + ") in " + corpusIDIndexFile);
		}

		return pointID;
	}

}
