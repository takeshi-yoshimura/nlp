package ac.keio.sslab.clustering.topdown;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main extends AbstractJob {

	Logger log = LoggerFactory.getLogger(getClass());
	
	public void runPreprocess(Configuration conf, Path inputDir, Path outputDir, Path preprocessOutput) 
			throws IOException, ClassNotFoundException, InterruptedException {
		PreProcessDriver.run(conf, inputDir, preprocessOutput);

		FileSystem fs = FileSystem.get(conf);
		IntWritable key = new IntWritable(); 
		VectorWritable value = new VectorWritable();
		TopDownKMeansCluster firstCluster = new TopDownKMeansCluster(1);
		for (FileStatus status: fs.listStatus(preprocessOutput)) {
			if (status.isDirectory() || status.getLen() == 0) //avoid reading _SUCCESS
				continue;
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), conf);
			while (reader.next(key, value)) {
				firstCluster.observe(value.get());
			}
			reader.close();
		}

		Path firstClustersFinal = new Path(outputDir, "topdown-0/iteration-0-final.seq");
		key.set(1);	
		ClusterWritable outValue = new ClusterWritable(firstCluster);
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, firstClustersFinal, IntWritable.class, ClusterWritable.class);
		writer.append(key, outValue);
		writer.close();
	}

	public int run(Path inputDir, Path outputDir, int maxIteration)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		Path preprocessOutput = new Path(outputDir, "topdown-0/clusteredPoints");
		if (!fs.exists(preprocessOutput)) {
			runPreprocess(conf, inputDir, outputDir, preprocessOutput);
		}
		
		Path nextInput = preprocessOutput;
		for (int i = 1; i <= maxIteration; i++) {
			Path nextOutput = new Path(outputDir, "topdown-" + i);
			Path clusteredPoints = new Path(nextOutput, "clusteredPoints");
			if (!fs.exists(clusteredPoints)) {
				Path kmeansFinal = KMeansDriver.run(conf, nextInput, nextOutput);
				KMeansClassifierDriver.run(conf, nextInput, clusteredPoints, kmeansFinal);
			}
			nextInput = clusteredPoints;
		}
		return 0;
	}

	@Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		addOption("maxIter", "x", "# of Iteration of topdown clustering", true);

		if (parseArguments(args) == null) {
			return -1;
		}
		int x = Integer.parseInt(getOption("maxIter"));
		return this.run(getInputPath(), getOutputPath(), x);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Main(), args);
	}
}
