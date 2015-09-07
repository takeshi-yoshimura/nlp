package ac.keio.sslab.nlp.corpus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.JobUtils;
import ac.keio.sslab.utils.hadoop.SequenceSwapWriter;

public class PatchCorpusWriter {

	// <patchText, pointID>
	Map<String, Integer> contentHash;
	// <pointID, [patchIDs]>
	Map<Integer, List<String>> idIndex;
	Map<String, Integer> df;

	DocumentFilter filter;

	File originalCorpus, dfFile, idIndexFile, patchFile, statsFile;
	PrintWriter originalCorpusWriter, patchWriter;
	boolean doForceWrite, splitParagraph, tokenizeAtUnderline, useNLTKStopwords;
	int totalDocuments, totalPatches;
	String startAt;

	public PatchCorpusWriter(File outputDir, boolean doForceWrite, boolean splitParagraph, boolean tokenizeAtUnderline, boolean useNLTKStopwords) throws IOException {
		contentHash = new HashMap<>();
		idIndex = new TreeMap<>();
		df = new HashMap<>();
		this.filter = new DocumentFilter(tokenizeAtUnderline, useNLTKStopwords);

		originalCorpus = new File(outputDir, "beforesStopWordsCorpus.txt");
		originalCorpusWriter = JobUtils.getPrintWriter(originalCorpus);
		dfFile = new File(outputDir, "df.txt");
		idIndexFile = new File(outputDir, "idIndex.txt");
		patchFile = new File(outputDir, "commits.txt");
		patchWriter = JobUtils.getPrintWriter(patchFile);
		statsFile = new File(outputDir, "stats.txt");
		totalDocuments = totalPatches = 0;
		this.doForceWrite = doForceWrite;
		this.splitParagraph = splitParagraph;
		this.tokenizeAtUnderline = tokenizeAtUnderline;
		this.useNLTKStopwords = useNLTKStopwords;

		JobUtils.promptDeleteDirectory(outputDir, doForceWrite);
		startAt = new Date().toString();
	}

	public void processPatchMessage(String patchID, String date, String version, Set<String> files, String message) throws Exception {
		boolean isProcessed = false;
		if (splitParagraph) {
			isProcessed = processPatchMessageSplitByParagraph(patchID, date, version, files, message);
		} else {
			isProcessed = processPatchMessageSimple(patchID, date, version, files, message);
		}
		if (isProcessed) {
			emitPatchMetaData(patchID, date, version, files);
			++totalPatches;
		}
	}

	protected boolean processPatchMessageSimple(String patchID, String date, String version, Set<String> files, String message) throws Exception {
		StringBuilder sb = new StringBuilder();
		for (String para: filter.filterDocument(message)) {
			sb.append(para).append(' ');
		}
		if (sb.length() == 0) {
			return false;
		}
		sb.setLength(sb.length() - 1);
		if (!sb.toString().contains(" ")) {
			return false;
		}
		addDocument(patchID, sb.toString());
		return true;
	}

	protected boolean processPatchMessageSplitByParagraph(String patchID, String date, String version, Set<String> files, String message) throws Exception {
		boolean hasParagraph = false;
		int paragraphID = 0;
		for (String para: filter.filterDocument(message)) {
			if (!para.contains(" ")) {
				continue;
			}
			if (!para.isEmpty()) {
				hasParagraph = true;
			}
			addDocument(patchID + "-" + paragraphID, para);
		}
		if (!hasParagraph) {
			return false;
		}
		emitPatchMetaData(patchID, date, version, files);
		return true;
	}

	protected void emitPatchMetaData(String patchID, String date, String version, Set<String> files) throws IOException {
		patchWriter.print(patchID);
		patchWriter.print(',');
		patchWriter.print(date);
		patchWriter.print(',');
		patchWriter.print(version);
		for (String file: files) {
			patchWriter.print(',');
			patchWriter.print(file);
		}
		patchWriter.println();
	}

	protected void addDocument(String documentID, String message) throws Exception {
		String hash = JobUtils.getSha(message);
		if (!contentHash.containsKey(hash)) {
			int pointID = idIndex.size();
			idIndex.put(pointID, new ArrayList<String>());
			contentHash.put(hash, pointID);
			originalCorpusWriter.print(Integer.toString(pointID));
			originalCorpusWriter.print(' ');
			originalCorpusWriter.println(message);

			Set<String> words = new HashSet<String>();
			for (String word: message.split(" ")) {
				words.add(word);
			}
			for (String word: words) {
				if (!df.containsKey(word)) {
					df.put(word, 0);
				}
				df.put(word, df.get(word) + 1);
			}
		}
		idIndex.get(contentHash.get(hash)).add(documentID);
		totalDocuments++;
	}

	protected Set<String> emitDF() throws IOException {
		Comparator<Entry<String, Integer>> reverser = new Comparator<Entry<String, Integer>>() {
			public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		};
		List<Entry<String, Integer>> orderedDf = new ArrayList<Entry<String, Integer>>(df.entrySet());
		Collections.sort(orderedDf, reverser);
		Set<String> stopWord = new HashSet<>();
		PrintWriter dfWriter = JobUtils.getPrintWriter(dfFile);		
		for (Entry<String, Integer> e: orderedDf) {
			if (e.getValue() > idIndex.size() / 5) {
				stopWord.add(e.getKey());
				dfWriter.print("[stop word] ");
			}
			dfWriter.print(e.getKey());
			dfWriter.print(',');
			dfWriter.println(e.getValue());
		}
		dfWriter.close();
		return stopWord;
	}

	protected void emitHDFSFile(FileSystem fs, Path outputPath, Path tmpPath, Set<String> stopWord) throws IOException {
		SequenceSwapWriter<String, String> writer = new SequenceSwapWriter<>(outputPath, tmpPath, fs, doForceWrite, String.class, String.class);
		BufferedReader br = new BufferedReader(new FileReader(originalCorpus));
		String line = null;
		StringBuilder sb = new StringBuilder();
		while ((line = br.readLine()) != null) {
			String [] splitLine = line.split(" ");
			sb.setLength(0);
			for (int j = 1; j < splitLine.length; j++) {
				if (stopWord.contains(splitLine[j])) {
					continue;
				}
				sb.append(splitLine[j]).append(' ');
			}
			if (sb.length() <= 1) {
				System.err.println("ignored line: " + line + " in " + originalCorpus.getAbsolutePath());
				continue;
			}
			sb.setLength(sb.length() - 1);
			writer.append(splitLine[0], sb.toString());
		}
		br.close();
		writer.close();
	}

	protected void emitIdIndex() throws IOException {
		PrintWriter idIndexWriter = JobUtils.getPrintWriter(idIndexFile);
		StringBuilder sb = new StringBuilder();
		for (Entry<Integer, List<String>> id: idIndex.entrySet()) {
			sb.setLength(0);
			sb.append(id.getKey()).append('\t').append('\t');
			for (String sha: id.getValue()) {
				sb.append(sha).append(',');
			}
			sb.setLength(sb.length() - 1);
			idIndexWriter.println(sb.toString());
		}
		idIndexWriter.close();
	}

	public void emitStats(String extraString) throws IOException {
		PrintWriter statsWriter = JobUtils.getPrintWriter(statsFile);
		statsWriter.println(extraString);
		statsWriter.print("total patch: ");
		statsWriter.println(totalPatches);
		statsWriter.print("total documents:");
		statsWriter.println(totalDocuments);
		statsWriter.print("total documents after deduplication: ");
		statsWriter.println(idIndex.size());
		statsWriter.print("tokenize at underline?: ");
		statsWriter.println(tokenizeAtUnderline);
		statsWriter.print("use NLTK stopword?: ");
		statsWriter.println(useNLTKStopwords);
		statsWriter.print("split paragraph?: ");
		statsWriter.println(splitParagraph);
		statsWriter.print("start at:");
		statsWriter.println(startAt);
		statsWriter.print("end at:");
		statsWriter.println(new Date().toString());
		statsWriter.close();
	}

	public void emitSummary(FileSystem fs, Path outputPath, Path tmpPath, String extraString) throws IOException {
		Set<String> stopWord = emitDF();
		emitHDFSFile(fs, outputPath, tmpPath, stopWord);
		emitIdIndex();
		emitStats(extraString);
	}

	public void close() {
		originalCorpusWriter.close();
	}
}
