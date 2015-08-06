package ac.keio.sslab.nlp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.mahout.common.Pair;

import ac.keio.sslab.nlp.lda.DocumentGroupReader;
import ac.keio.sslab.nlp.lda.LDAHDFSFiles;

public class ExtractGroupJob implements NLPJob {

	@Override
	public String getJobName() {
		return "extractGroup";
	}

	@Override
	public String getJobDescription() {
		return "extract specified group of document IDs";
	}

	@Override
	public Options getOptions() {
		OptionGroup g = new OptionGroup();
		g.addOption(new Option("f", "groupFileName", true, "file that contains document IDs (plain text)"));
		g.addOption(new Option("l", "ldaID", true, "LDA ID"));
		g.setRequired(true);

		Options opts = new Options();
		opts.addOptionGroup(g);
		return opts;
	}

	@Override
	public void run(JobManager mgr) {
		NLPConf conf = mgr.getNLPConf();
		File inputFile = new File(mgr.getArgStr("f"));
		File groupFile = new File(conf.finalOutputFile, "group");
		File outputFile = mgr.getLocalArgFile(groupFile, "l");
		LDAHDFSFiles hdfs = new LDAHDFSFiles(mgr.getArgJobIDPath(conf.ldaPath, "l"));
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			Set<Integer> group = new HashSet<Integer>();
			String line = reader.readLine();
			while (line != null) {
				group.add(Integer.parseInt(line));
				line = reader.readLine();
			} 
			reader.close();

			File documentFile = new File(outputFile, inputFile.getName());
			System.out.println("Extracting documents with top 10 topics: " + documentFile.getAbsolutePath());
			PrintWriter pw2 = JobUtils.getPrintWriter(documentFile);
			DocumentGroupReader docReader = new DocumentGroupReader(hdfs.docIndexPath, hdfs.documentPath, group, conf.hdfs, 10);
			StringBuilder sb = new StringBuilder();
			sb.append("centroid\t");
			for (int topicId: docReader.getCentroid(10)) {
				if (sb.length() > "centroid\t".length()) {
					sb.append(' ');
				}
				sb.append(topicId);
			}
			pw2.println(sb.toString());
			for (Pair<String, List<Integer>> document: docReader.getDocuments()) {
				sb.setLength(0);
				sb.append(document.getFirst()).append('\t');
				int len = sb.length();
				for (int topicId: document.getSecond()) {
					if (sb.length() > len) {
						sb.append(' ');
					}
					sb.append(topicId);
				}
				pw2.println(sb.toString());
			}
			pw2.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Failed: " + e.toString());
		}
	}

	@Override
	public boolean runInBackground() {
		return false;
	}

}
