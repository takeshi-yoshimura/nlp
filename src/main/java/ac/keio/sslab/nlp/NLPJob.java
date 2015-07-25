package ac.keio.sslab.nlp;

import java.util.Map;

import org.apache.commons.cli.Options;

public interface NLPJob {
	/**
	 * Get the job name
	 * 
	 * @return the job name
	 */
	public String getJobName();
	
	/**
	 * Get the job description.
	 * 
	 * @return the job description
	 */
	public String getJobDescription();

	/**
	 * Get options for running the job.
	 * 
	 * @return keys are short name of parameters, values are long names of parameters
	 */
	public Options getOptions();

	/**
	 * Run a job
	 * 
	 * @param args the parsed arguments into the form of (key, value). 
	 *             key is option name, value is the specified argument string.
	 */
	public void run(Map<String, String> args);

	public boolean runInBackground();
}
