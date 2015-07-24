package ac.keio.sslab.clustering.topdown;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
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
import org.apache.mahout.math.VectorWritable;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;

public class KMeansClassifierDriver {
	static int numClusterDivision = 2;

	static public void run(Configuration conf, Path input, Path output,
			Path kmeansFinal) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "KMeans Classifier over input: " + input);
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setMapperClass(KMeansClassifierMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, output);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);

		SideData.setNumClusterDivision(job.getConfiguration(),
				numClusterDivision);
		SideData.setKMeansFinalPath(job.getConfiguration(), kmeansFinal);

		job.setJarByClass(KMeansClassifierMapper.class);

		if (!job.waitForCompletion(true))
			throw new InterruptedException("job failure at" + job.getJobName());
	}

	static public class SideData {
		static final String prefix = "jp.ac.keio.ics.sslab.yoshimura.kmeansclassifier";

		static final String numClusterDivision = prefix + "numClusterDivision";

		static public void setNumClusterDivision(Configuration conf, int value) {
			conf.setInt(numClusterDivision, value);
		}

		static public int getNumClusterDivision(Configuration conf) {
			return Integer.parseInt(conf.get(numClusterDivision));
		}

		static final String kmeansFinalDirLink = "kmeansFinalPath";

		static public void setKMeansFinalPath(Configuration conf, Path value) {
			DistributedCache.addCacheFile(URI.create(value.toString() + "#" + kmeansFinalDirLink), conf);
			DistributedCache.createSymlink(conf);
		}

		static public Map<Integer, Cluster> getFinalClusterMap(
				Configuration conf) throws IOException {
			IntWritable inKey = new IntWritable();
			ClusterWritable inValue = new ClusterWritable();
			Map<Integer, Cluster> map = new HashMap<Integer, Cluster>();
			FileSystem fs = FileSystem.getLocal(conf);
			
			for (FileStatus status: fs.listStatus(new Path(kmeansFinalDirLink))) {
				if (status.isDirectory() || status.getLen() == 0) //avoid reading _SUCCESS
					continue;
				SequenceDirectoryReader reader = new SequenceDirectoryReader(status.getPath(), conf);
				while (reader.next(inKey, inValue)) {
					map.put(inKey.get(), inValue.getValue());
				}
				reader.close();
			}
			if (map.isEmpty())
				throw new IOException("Clusters not found");
			return map;
		}
	}

}
