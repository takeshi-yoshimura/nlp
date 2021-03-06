package ac.keio.sslab.nlp.job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.eclipse.jgit.util.FileUtils;

import ac.keio.sslab.nlp.lda.LDAFiles;
import ac.keio.sslab.utils.SimpleSorter;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;
import ac.keio.sslab.utils.hadoop.SequenceSwapWriter;

public class JobUtils {

	public static boolean isSucceededMapReduce(FileSystem fs, Path mapReduceOutputPath) throws IOException {
		return fs.exists(new Path(mapReduceOutputPath, LDAFiles.mapReduceSuccessFileName));
	}

	public static PrintWriter getPrintWriter(File file) throws IOException {
		file.getParentFile().mkdirs();
		return new PrintWriter(new BufferedWriter(new FileWriter(file)));
	}

	public static void saveArguments(FileSystem fs, Path argPath, String[] args) throws IOException {
		NLPConf conf = NLPConf.getInstance();
		SequenceSwapWriter<String, Void> writer = new SequenceSwapWriter<>(argPath, conf.tmpPath, fs, true, String.class, Void.class);
		for (String arg: args) {
			writer.append(arg, null);
		}
		writer.close();
	}

	public static String[] restoreArguments(FileSystem fs, Path argPath) throws IOException {
		List<String> list = new ArrayList<String>();
		SequenceDirectoryReader<String, Void> reader = new SequenceDirectoryReader<>(argPath, fs.getConf(), String.class, Void.class);
		while (reader.seekNextKey()) {
			list.add(reader.key());
		}
		reader.close();
		return list.toArray(new String[list.size()]);
	}

	public static boolean promptDeleteDirectory(FileSystem fs, Path outputPath, boolean force) {
		try {
			if (!fs.exists(outputPath)) {
				return true;
			}
			if (!force) {
				while (true) {
					System.out.println("Do you really want to overwrite existing directory " + outputPath.toString() + " [Yes/No]");
					BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
					String result;
					result = br.readLine();
					if (result.equals("Yes")) {
						break;
					} else if (result.equals("No")) {
						return false;
					} else {
						System.out.println("Type Yes or No");
					}
				}
			}
			System.out.println("Delete existing directory " + outputPath.toString());
			fs.delete(outputPath, true);
			return true;
		} catch (IOException e) {
			System.err.println("HDFS or local I/O failed : " + e.toString());
			return false;
		}
	}

	public static boolean promptDeleteDirectory(File outputFile, boolean force) {
		try {
			if (!outputFile.exists()) {
				return true;
			}
			if (!force) {
				while (true) {
					System.out.println("Do you really want to overwrite existing directory " + outputFile.getAbsolutePath() + " [Yes/No]");
					BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
					String result;
					result = br.readLine();
					if (result.equals("Yes")) {
						break;
					} else if (result.equals("No")) {
						return false;
					} else {
						System.out.println("Type Yes or No");
					}
				}
			}
			System.out.println("Delete existing directory " + outputFile.getAbsolutePath());
			FileUtils.delete(outputFile.getAbsoluteFile(), FileUtils.RECURSIVE);
			return true;
		} catch (IOException e) {
			System.err.println("Local I/O failed : " + e.toString());
			return false;
		}
	}

	public static String getSha(String str) throws Exception {
		MessageDigest md = MessageDigest.getInstance("SHA-512");
	    md.update(str.getBytes());
	    byte[] hash = md.digest();
	    StringBuilder sb = new StringBuilder();
	    for (byte b : hash) {
	        sb.append(String.format("%02x", b));
	    }
	    return sb.toString();
	}

	public static List<Entry<Integer, Double>> getTopElements(Vector v, int num) {
		Map<Integer, Double> points = new HashMap<Integer, Double>();
		for (Vector.Element e: v.nonZeroes()) {
			points.put(e.index(), e.get());
		}
		List<Entry<Integer, Double>> sorted = SimpleSorter.reverse(points);
		List<Entry<Integer, Double>> ret = new ArrayList<Entry<Integer, Double>>();
		for (int i = 0; i < sorted.size() && i < num; i++) {
			ret.add(sorted.get(i));
		}
		return ret;
	}
}
