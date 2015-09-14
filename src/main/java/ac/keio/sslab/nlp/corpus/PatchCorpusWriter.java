package ac.keio.sslab.nlp.corpus;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
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

import ac.keio.sslab.nlp.job.JobUtils;
import ac.keio.sslab.utils.SimpleSorter;
import ac.keio.sslab.utils.hadoop.SequenceSwapWriter;

public class PatchCorpusWriter {

	// <patchText, pointID>
	Map<String, Integer> contentHash;
	// <pointID, [patchIDs]>
	Map<Integer, List<String>> idIndex;
	Map<String, Integer> df;

	DocumentFilter filter;

	File originalCorpus, beforeStopWordCorpus, dfFile, idIndexFile, patchFile, statsFile;
	PrintWriter beforeStopWordWriter, patchWriter;
	boolean doForceWrite, splitParagraph, tokenizeAtUnderline, useNLTKStopwords;
	int totalDocuments, totalPatches;
	String startAt;

	public PatchCorpusWriter(File outputDir, boolean doForceWrite, boolean splitParagraph, boolean tokenizeAtUnderline, boolean useNLTKStopwords) throws IOException {
		contentHash = new HashMap<>();
		idIndex = new TreeMap<>();
		df = new HashMap<>();
		this.filter = new DocumentFilter(tokenizeAtUnderline, useNLTKStopwords);

		JobUtils.promptDeleteDirectory(outputDir, doForceWrite);

		originalCorpus = new File(outputDir, "originalCorpus");
		originalCorpus.mkdirs();
		beforeStopWordCorpus = new File(outputDir, "beforeStopWordCorpus.txt");
		beforeStopWordWriter = JobUtils.getPrintWriter(beforeStopWordCorpus);
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
		startAt = new Date().toString();
	}

	public void process(RepositoryReader reader) throws Exception {
		processPatchMessage(reader.getID(), reader.getDate(), reader.getVersion(), reader.getFiles(), reader.getDoc());
	}

	protected void processPatchMessage(String patchID, String date, String version, Set<String> files, String message) throws Exception {
		boolean isProcessed = false;
		if (splitParagraph) {
			isProcessed = processPatchMessageSplitByParagraph(patchID, date, version, files, message);
		} else {
			isProcessed = processPatchMessageSimple(patchID, date, version, files, message);
		}
		if (isProcessed) {
			emitPatchMetaData(patchID, date, version, files);
			emitOriginalCorpus(patchID, message);
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

	protected void emitOriginalCorpus(String patchID, String message) throws IOException {
		PrintWriter originalCorpusWriter = JobUtils.getPrintWriter(new File(originalCorpus, patchID));
		originalCorpusWriter.println(message);
		originalCorpusWriter.close();
	}

	protected void addDocument(String documentID, String message) throws Exception {
		String hash = JobUtils.getSha(message);
		if (!contentHash.containsKey(hash)) {
			int pointID = idIndex.size();
			idIndex.put(pointID, new ArrayList<String>());
			contentHash.put(hash, pointID);
			beforeStopWordWriter.print(Integer.toString(pointID));
			beforeStopWordWriter.print(' ');
			beforeStopWordWriter.println(message);

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
		List<Entry<String, Integer>> orderedDf = SimpleSorter.reverse(df);
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
		BeforeStopWordCorpusReader r = new BeforeStopWordCorpusReader(beforeStopWordCorpus.getParentFile());
		StringBuilder sb = new StringBuilder();
		while (r.seekNext()) {
			for (String w: r.val()) {
				if (stopWord.contains(w)) {
					continue;
				}
				sb.append(w).append(' ');
			}
			if (sb.length() <= 1) {
				System.err.println("ignored line: key = " + r.key() + " in " + beforeStopWordCorpus.getAbsolutePath());
				continue;
			}
			sb.setLength(sb.length() - 1);
			writer.append(Integer.toString(r.key()), sb.toString());
		}
		r.close();
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
		beforeStopWordWriter.close();
		patchWriter.close();
	}
}
