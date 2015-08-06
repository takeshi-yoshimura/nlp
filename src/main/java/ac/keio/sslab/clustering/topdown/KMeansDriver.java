package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;
import ac.keio.sslab.hadoop.utils.SequenceSwapWriter;
import ac.keio.sslab.nlp.NLPConf;

public class KMeansDriver {

	static int numClusterDivision = 2;
	static int maxIteration = 100;
	static int convergenceConfirmEvery = 5;
	static int numRetry = 10;
	
	static public void doKMeansMR(Job job, Path input, Path iterationOutput, Path oldOutput, Path oldoldOutput) throws IOException, ClassNotFoundException, InterruptedException {
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(KMeansMapper.class);
		job.setReducerClass(KMeansReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ClusterWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, iterationOutput);

		job.setJarByClass(KMeansMapper.class);

		SideData.setNumClusterDivision(job.getConfiguration(), numClusterDivision);
		SideData.setOldOutputDirPath(job.getConfiguration(), oldOutput);
		if (oldoldOutput != null)
			SideData.setOldOldOutputDirPath(job.getConfiguration(), oldoldOutput);

		if (!job.waitForCompletion(true))
			throw new InterruptedException("job failure at" + job.getJobName());
	}
	
	static public void combineKMeansOutput(SequenceSwapWriter<Integer, Cluster> writer, Path iterationOutput, Path oldOutput, Path oldoldOutput) throws IOException {
		FileSystem fs = NLPConf.getInstance().hdfs;
		Configuration conf = NLPConf.getInstance().hadoopConf;
		//combine split files into a file for better performance
		FileStatus [] newStat = fs.listStatus(iterationOutput);
		FileStatus [] oldStat = fs.listStatus(oldOutput);
		FileStatus [] oldoldStat = fs.listStatus(oldoldOutput);
		for (int s = 0; s < newStat.length && s < oldStat.length && s < oldoldStat.length; s++) {
			SequenceDirectoryReader<Integer, Cluster> newReader = null;
			SequenceDirectoryReader<Integer, Cluster> oldReader = null;
			SequenceDirectoryReader<Integer, Cluster> oldoldReader = null;
			//put the same key records near each other
			if (s < newStat.length && !newStat[s].isDirectory() && newStat[s].getLen() > 0)
				newReader = new SequenceDirectoryReader<>(newStat[s].getPath(), conf, Integer.class, Cluster.class);
			if (s < oldStat.length && !oldStat[s].isDirectory() && oldStat[s].getLen() > 0)
				oldReader = new SequenceDirectoryReader<>(oldStat[s].getPath(), conf, Integer.class, Cluster.class);
			if (s < oldoldStat.length && !oldoldStat[s].isDirectory() && oldoldStat[s].getLen() > 0)
				oldoldReader = new SequenceDirectoryReader<>(oldoldStat[s].getPath(), conf, Integer.class, Cluster.class);

			boolean newFin, oldFin, oldoldFin;
			newFin = oldFin = oldoldFin = false;
			do {
				if (newReader != null)
					newFin = newReader.seekNext();
				if (newFin)
					writer.append(newReader.keyW(), newReader.valW());
				if (oldReader != null)
					oldFin = oldReader.seekNext();
				if (oldFin)
					writer.append(oldReader.keyW(), oldReader.valW());
				if (oldoldReader != null)
					oldoldFin = oldoldReader.seekNext();
				if (oldoldFin)
					writer.append(oldoldReader.keyW(), oldoldReader.valW());
			} while (newFin || oldFin || oldoldFin);
			if (newReader != null) newReader.close();
			if (oldReader != null) oldReader.close();
			if (oldoldReader != null) oldoldReader.close();
		}
	}
	
	static public Path KMeansMainLoop(Path input, Path output) throws IOException, ClassNotFoundException, InterruptedException {
		List<Path> finalOutputs = new ArrayList<Path>();
		Path initialOutput = new Path(output, "iteration-0/clusters");
		NLPConf conf = NLPConf.getInstance();
		KMeansInitDriver.run(conf.hadoopConf, input, initialOutput);

		Path oldOutput = initialOutput;
		Path oldoldOutput = null;
		int i;
		for (i = 1; i <= maxIteration; i++) {
			Path iterationDir = new Path(output, "iteration-" + i);
			Path iterationOutput = new Path(iterationDir, "clusters");
			Job job = new Job(conf.hadoopConf, "KMeans Iteration #" + i + " over input:" + input);
			doKMeansMR(job, input, iterationOutput, oldOutput, oldoldOutput);

			//decide if we can stop iterations or not
			if (oldoldOutput != null && i % convergenceConfirmEvery == 0) {
				//combine split files into a file for better performance
				Path tmpInput = new Path(iterationDir, "tmpInputForConvergenceCalc.seq");

				SequenceSwapWriter<Integer, Cluster> writer = new SequenceSwapWriter<>(tmpInput, conf.tmpPath, conf.hdfs, true, Integer.class, Cluster.class);
				combineKMeansOutput(writer, iterationOutput, oldOutput, oldoldOutput);
				writer.close();

				Path convergenceOutput = new Path(iterationDir, "convergence");
				long unconvergedCount = CalcConvergenceDriver.run(conf.hadoopConf, tmpInput, convergenceOutput);
				Path convergedPath = new Path(convergenceOutput, "converged");
				if (conf.hdfs.exists(convergedPath))
					finalOutputs.add(convergedPath);
				if (unconvergedCount == 0) {
					oldOutput = null;
					break;
				}
				oldoldOutput = oldOutput;
				oldOutput = new Path(convergenceOutput, "unconverged");
			} else {
				oldoldOutput = oldOutput;
				oldOutput = iterationOutput;
			}
		}

		//in case of iterations stopped by maxIteration
		if (finalOutputs.isEmpty()) //no converged clusters
			return oldOutput;
		else if (oldOutput != null)
			finalOutputs.add(oldOutput);

		//combine converged files split across iteration-X dirs
		i++;
		Path finalOutput = new Path(output, "iteration-" + i + "-final.seq");
		SequenceSwapWriter<Integer, Cluster> writer = new SequenceSwapWriter<>(finalOutput, conf.tmpPath, conf.hdfs, true, Integer.class, Cluster.class);
		for (Path out: finalOutputs) {
			for (FileStatus status: conf.hdfs.listStatus(out)) {
				if (status.isDirectory() || status.getLen() == 0) //avoid reading _SUCCESS
					continue;
				SequenceDirectoryReader<Integer, Cluster> reader = new SequenceDirectoryReader<>(status.getPath(), conf.hdfs, Integer.class, Cluster.class);
				while (reader.seekNext()) {
					writer.append(reader.keyW(), reader.valW());
				}
				reader.close();
			}
		}
		writer.close();
		return finalOutput;
	}

	static public Path run(Path input, Path output) throws IOException, ClassNotFoundException,	InterruptedException {
		int retry;
		Path bestResult = null;
		double lowestRSS = Double.MAX_VALUE;
		NLPConf conf = NLPConf.getInstance();

		int startFrom = 0;
		//check if there are any suspended jobs
		if (conf.hdfs.exists(output)) {
			for (FileStatus status: conf.hdfs.listStatus(output)) {
				String pathName = status.getPath().getName();
				if (status.isDirectory() && pathName.matches("retry-[0-9]+")) {
					int candidate = Integer.parseInt(pathName.substring(6)) + 1;
					if (startFrom < candidate) {
						startFrom = candidate;
					}
				}
			}
		}

		for (retry = startFrom; retry < numRetry; retry++) {
			Path retryOutput = new Path(output, "retry-" + retry);
			Path mainLoopOutput = KMeansMainLoop(input, retryOutput);

			//use the lowest RSS result for better clustering
			SequenceDirectoryReader<Integer, Cluster> reader = new SequenceDirectoryReader<>(mainLoopOutput, conf.hdfs, Integer.class, Cluster.class);
			double RSS = 0;
			while (reader.seekNext()) {
				RSS += ((TopDownKMeansCluster)reader.val()).getRSSk();
			}
			reader.close();
			if (RSS < lowestRSS) {
				if (lowestRSS != Double.MAX_VALUE) {
					//output is usually GB-order. need to save disk space
					conf.hdfs.delete(bestResult.getParent(), true);
				}
				lowestRSS = RSS;
				bestResult = mainLoopOutput;
			} else {
				//output is usually GB-order. need to save disk space
				conf.hdfs.delete(mainLoopOutput.getParent(), true);
			}
		}
		conf.hdfs.rename(new Path(bestResult.getParent(), "clusteredPoints"), new Path(output, "clusteredPoints"));

		Path finalOutput = new Path(output, "iteration-" + retry + "-final.seq");
		conf.hdfs.rename(bestResult, finalOutput);
		conf.hdfs.delete(bestResult.getParent(), true);
		return finalOutput;
	}

	static public class SideData {
		static final String prefix = "jp.ac.keio.ics.sslab.yoshimura.KMeans";

		static final String numClusterDivision = prefix + "numClusterDivision";

		static public void setNumClusterDivision(Configuration conf, int value) {
			conf.setInt(numClusterDivision, value);
		}

		static public int getNumClusterDivision(Configuration conf) {
			return Integer.parseInt(conf.get(numClusterDivision));
		}

		static final String oldOutputDirLink = "oldOutputLink";

		static public void setOldOutputDirPath(Configuration conf, Path value) {
			DistributedCache.addCacheFile(URI.create(value.toString() + "#" + oldOutputDirLink), conf);
			DistributedCache.createSymlink(conf);
		}

		static public Map<Integer, TopDownKMeansCluster> getOldClusterMap(Configuration conf) throws IOException, URISyntaxException {
			Map<Integer, TopDownKMeansCluster> map = new HashMap<Integer, TopDownKMeansCluster>();
			FileSystem fs = FileSystem.getLocal(conf);
			
			for (FileStatus status: fs.listStatus(new Path(oldOutputDirLink))) {
				if (status.isDirectory() || status.getLen() == 0) //avoid reading _SUCCESS
					continue;
				SequenceDirectoryReader<Integer, Cluster> reader = new SequenceDirectoryReader<>(status.getPath(), conf, Integer.class, Cluster.class);
				while (reader.seekNext()) {
					map.put(reader.key(), (TopDownKMeansCluster) reader.val());
				}
				reader.close();
			}
			if (map.isEmpty())
				throw new IOException("Clusters not found");
			return map;
		}

		static final String oldoldOutputDirLink = "oldoldOutputDirLink";

		static public void setOldOldOutputDirPath(Configuration conf, Path value) {
			DistributedCache.addCacheFile(URI.create(value.toString() + "#" + oldoldOutputDirLink), conf);
			DistributedCache.createSymlink(conf);
		}

		static public Map<Integer, TopDownKMeansCluster> getOldOldClusterMap(Configuration conf) throws IOException, URISyntaxException {
			Map<Integer, TopDownKMeansCluster> map = null;
			FileSystem fs = FileSystem.getLocal(conf);
			Path oldoldOutputDir = new Path(oldoldOutputDirLink);

			if (!fs.exists(oldoldOutputDir)) {
				return null;
			}
			map = new HashMap<Integer, TopDownKMeansCluster>();
			for (FileStatus status: fs.listStatus(oldoldOutputDir)) {
				if (status.isDirectory() || status.getLen() == 0) //avoid reading _SUCCESS
					continue;
				SequenceDirectoryReader<Integer, Cluster> reader = new SequenceDirectoryReader<>(status.getPath(), conf, Integer.class, Cluster.class);
				while (reader.seekNext()) {
					map.put(reader.key(), (TopDownKMeansCluster)reader.val());
				}
				reader.close();
			}
			return map;
		}
	}
}
