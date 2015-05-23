package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.eclipse.jgit.util.FileUtils;
import org.json.JSONObject;

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
		Writer writer = null;
		try {
			Writer.Option fileOpt = Writer.file(argPath);
			Writer.Option keyOpt = Writer.keyClass(Text.class);
			Writer.Option valueOpt = Writer.valueClass(NullWritable.class);
			Writer.Option compOpt = Writer.compression(CompressionType.NONE);
			writer = SequenceFile.createWriter(fs.getConf(), fileOpt, keyOpt, valueOpt, compOpt);
			Text key = new Text();
			NullWritable value = NullWritable.get();
			for (String arg: args) {
				key.set(arg);
				writer.append(key, value);
			}
			writer.close();
		} catch(Exception e) {
			try {
				writer.close();
				fs.delete(argPath, true);
			} catch (IOException e2) {
				throw new IOException("Writing parameters aborts: " + e.toString()
									+ "\nHowever deleting " + argPath + " also fails: " + e2.toString()
									+ "\nTry deleting " + argPath + " by manual.");
			}
			throw new IOException("Writing arguments aborts: " + e.toString());
		}
	}

	public static String[] restoreArguments(FileSystem fs, Path argPath) throws IOException {
		List<String> list = new ArrayList<String>();
		Reader reader = null;
		try {
			Reader.Option fileOpt = Reader.file(argPath);
			reader = new Reader(fs.getConf(), fileOpt);
			Text key = new Text();
			while (reader.next(key)) {
				list.add(key.toString());
			}
			reader.close();
		} catch(Exception e) {
			reader.close();
			throw new IOException("Reading arguments aborts: " + e.toString());
		}
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

	public static void runGNUPatch(File diffFile, File patchedDirectory) {
		ProcessBuilder pb = new ProcessBuilder();
		pb.directory(patchedDirectory);
		List<String> cmd = new ArrayList<String>();
		cmd.add("patch"); cmd.add("-p1"); cmd.add("--no-backup-if-mismatch"); cmd.add("-r"); cmd.add("-");
		cmd.add("-i"); cmd.add(diffFile.getAbsolutePath()); cmd.add("-f");
		pb.command(cmd);
		pb.inheritIO(); //use stdout, stdin, stderr
		Process p;
		try {
			p = pb.start();
		} catch (IOException e) {
			System.err.println("patch failed: " + e.toString());
			return;
		}
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			/* do nothing */
		}
		try {
			p.getInputStream().close();
			p.getErrorStream().close();
			p.getOutputStream().close();
			p.destroy();
		} catch (Exception e) {
			System.err.println("Error while closing file descriptors: " + e.toString());
		}
	}

	public static void listJobs(String jobName) {
		NLPConf conf = new NLPConf();
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
}
