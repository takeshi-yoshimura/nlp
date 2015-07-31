package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.jgit.util.FileUtils;
import org.json.JSONObject;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;
import ac.keio.sslab.nlp.lda.LDAFiles;

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
		SequenceSwapWriter<String, Void> writer = new SequenceSwapWriter<>(argPath, conf.tmpPath, new Configuration(), true, String.class, Void.class);
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

	public static void listJobs(String jobName) {
		NLPConf conf = NLPConf.getInstance();
		File argFile = new File(conf.localArgFile, jobName);
		argFile.getParentFile().mkdirs();
		try {
			if (!argFile.exists()) {
				System.out.println("Job " + jobName + " has never been invoked");
			} else {
				FileInputStream inputStream = new FileInputStream(argFile);
				JSONObject jobJson = new JSONObject(IOUtils.toString(inputStream));
				StringBuilder sb = new StringBuilder();
				for (String jobID: jobJson.keySet()) {
					sb.setLength(0);
					sb.append(jobID).append(":");
					JSONObject argObj = jobJson.getJSONObject(jobID);
					for (String arg: argObj.keySet()) {
						sb.append(" -").append(arg).append(" ").append(argObj.get(arg));
					}
					System.out.println(sb.toString());
				}
		        inputStream.close();
			}
		} catch (Exception e) {
			System.err.println("List up job IDs failed: " + e.toString());
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

	public static void addJarToDistributedCache(Class<?> classToAdd, Configuration conf) throws IOException {
		// Retrieve jar file for class2Add
		String jar = classToAdd.getProtectionDomain().getCodeSource().getLocation().getPath();
		File jarFile = new File(jar);

		// Declare new HDFS location
		Path hdfsJar = new Path(NLPConf.getInstance().tmpPath, "jar/" + jarFile.getName());

		// Mount HDFS
		FileSystem hdfs = FileSystem.get(conf);
		hdfs.mkdirs(hdfsJar.getParent());

		// Copy (override) jar file to HDFS
		hdfs.copyFromLocalFile(false, true, new Path(jar), hdfsJar);

		// Add jar to distributed classPath
		DistributedCache.addFileToClassPath(hdfsJar, conf);
	}
}
