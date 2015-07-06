package ac.keio.sslab.nlp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;

import ac.keio.sslab.hadoop.utils.SequenceDirectoryReader;

public class DeduplicateCorpusJob implements NLPJob {

	NLPConf conf = new NLPConf();
	@Override
	public String getJobName() {
		return "dedupCorpus";
	}

	@Override
	public String getJobDescription() {
		return "remove identical documents";
	}

	@Override
	public Options getOptions() {
		Options opts = new Options();
		opts.addOption("c", "corpusID", true, "ID of a corpus");
		return opts;
	}

	@Override
	public void run(Map<String, String> args) {
		if (!args.containsKey("c")) {
			System.err.println("Need to specify --corpusID");
			return;
		}
		Path corpusPath = new Path(conf.corpusPath, args.get("c"));
		Path outputPath = new Path(conf.corpusPath, args.get("j"));
		Configuration conf = new Configuration();
		try {
			Map<String, String> out = new HashMap<String, String>();//<value, key> in original corpus
			SequenceDirectoryReader reader = new SequenceDirectoryReader(corpusPath, conf);
			Text key = new Text(); Text value = new Text();
			while (reader.next(key, value)) {
				out.put(value.toString(), key.toString());
			}
			reader.close();

			Writer.Option fileOpt = Writer.file(outputPath);
			Writer.Option keyOpt = Writer.keyClass(Text.class);
			Writer.Option valueOpt = Writer.valueClass(Text.class);
			Writer.Option compOpt = Writer.compression(CompressionType.NONE);
			Writer writer = SequenceFile.createWriter(conf, fileOpt, keyOpt, valueOpt, compOpt);
			for (Entry<String, String> e: out.entrySet()) {
				key.set(e.getValue());
				value.set(e.getKey());
				writer.append(key, value);
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Reading or writing to HDFS failed: " + e.toString());
		}
	}

	@Override
	public void takeSnapshot() {
		/* do nothing */
	}

	@Override
	public void restoreSnapshot() {
		/* do nothing */
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
