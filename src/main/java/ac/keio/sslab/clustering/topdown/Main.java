package ac.keio.sslab.clustering.topdown;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ac.keio.sslab.nlp.NLPConf;
import ac.keio.sslab.utils.hadoop.SequenceDirectoryReader;
import ac.keio.sslab.utils.hadoop.SequenceSwapWriter;

public class Main extends AbstractJob {

	Logger log = LoggerFactory.getLogger(getClass());
	NLPConf conf = NLPConf.getInstance();

	public void runPreprocess(Configuration hdfsConf, Path inputDir, Path outputDir, Path preprocessOutput)  throws IOException, ClassNotFoundException, InterruptedException {
		PreProcessDriver.run(hdfsConf, inputDir, preprocessOutput);
		TopDownKMeansCluster firstCluster = new TopDownKMeansCluster(1);
		for (FileStatus status: conf.hdfs.listStatus(preprocessOutput)) {
			if (status.isDirectory() || status.getLen() == 0) //avoid reading _SUCCESS
				continue;
			SequenceDirectoryReader<Integer, Vector> reader = new SequenceDirectoryReader<>(status.getPath(), hdfsConf, Integer.class, Vector.class);
			while (reader.seekNext()) {
				firstCluster.observe(reader.val());
			}
			reader.close();
		}

		Path firstClustersFinal = new Path(outputDir, "topdown-0/iteration-0-final.seq");
		SequenceSwapWriter<Integer, Cluster> writer = new SequenceSwapWriter<>(firstClustersFinal, conf.tmpPath, conf.hdfs , true, Integer.class, Cluster.class);
		writer.append(1, firstCluster);
		writer.close();
	}

	public int run(Path inputDir, Path outputDir, int maxIteration) throws IOException, ClassNotFoundException, InterruptedException {
		Path preprocessOutput = new Path(outputDir, "topdown-0/clusteredPoints");
		if (!conf.hdfs.exists(preprocessOutput)) {
			runPreprocess(conf.hadoopConf, inputDir, outputDir, preprocessOutput);
		}

		Path nextInput = preprocessOutput;
		for (int i = 1; i <= maxIteration; i++) {
			Path nextOutput = new Path(outputDir, "topdown-" + i);
			Path clusteredPoints = new Path(nextOutput, "clusteredPoints");
			if (!conf.hdfs.exists(clusteredPoints)) {
				Path kmeansFinal = KMeansDriver.run(nextInput, nextOutput);
				KMeansClassifierDriver.run(conf.hadoopConf, nextInput, clusteredPoints, kmeansFinal);
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
