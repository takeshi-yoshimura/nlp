package ac.keio.sslab.nlp;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import ac.keio.sslab.nlp.corpus.PatchCorpusReader;
import ac.keio.sslab.nlp.corpus.PatchCorpusReader.PatchEntry;
import ac.keio.sslab.nlp.corpus.PatchCorpusWriter;

public class MergeCorporaJob implements NLPJob {

	@Override
	public String getJobName() {
		return "mergeCorpus";
	}

	@Override
	public String getJobDescription() {
		return "merge separated corpora into one";
	}

	@Override
	public Options getOptions() {
		OptionGroup required = new OptionGroup();
		required.addOption(new Option("c", "corpusIDs", true, "IDs of merged corpora (separated by ',')"));
		required.setRequired(true);

		Options opt = new Options();
		opt.addOptionGroup(required);
		return opt;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = NLPConf.getInstance();
		Path outputPath = mgr.getJobIDPath(conf.corpusPath);
		String [] corpora = mgr.getArgStr("c").split(",");
		File output = new File(conf.localCorpusFile, mgr.getArgStr("j"));

		try {
			PatchCorpusWriter writer = new PatchCorpusWriter(output, mgr.doForceWrite(), false, false, false);

			StringBuilder sb = new StringBuilder();
			for (String corpusID: corpora) {
				System.out.println("read corpus job name: " + corpusID);
				sb.append(corpusID).append(',');
				File corpusDir = new File(conf.localCorpusFile, corpusID);
				PatchCorpusReader reader = new PatchCorpusReader(corpusDir);
				Map<Integer, List<String>> idIndex = reader.getIdIndex();
				Map<Integer, String> documents = reader.getOriginalDocuments();
				Map<String, PatchEntry> patchEntries = reader.getPatchEntries();
				for (Entry<Integer, List<String>> e: idIndex.entrySet()) {
					String message = documents.get(e.getKey());
					for (String hash: e.getValue()) {
						PatchEntry p = patchEntries.get(hash);
						writer.processPatchMessage(hash, p.date, p.ver, p.files, message);
					}
				}
			}
			if (sb.length() > 1) {
				sb.setLength(sb.length() - 1);
				writer.emitSummary(conf.hdfs, outputPath, conf.tmpPath, "merged: " + sb.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}

	@Override
	public boolean runInBackground() {
		// TODO Auto-generated method stub
		return false;
	}

}
