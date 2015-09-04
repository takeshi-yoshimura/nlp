package ac.keio.sslab.nlp.corpus;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.util.CharacterUtils;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.analysis.util.CharacterUtils.CharacterBuffer;
import org.apache.lucene.util.Version;

public class ParagraphAnalyzer extends Analyzer {

	@Override
	protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
		ParagraphTokenizer tokenizer = new ParagraphTokenizer(Version.LUCENE_46, reader);
		TokenStream filter = new LowerCaseFilter(Version.LUCENE_46, tokenizer);
		filter = new SignatureFilter(Version.LUCENE_46, filter);
		return new TokenStreamComponents(tokenizer, filter);
	}

}

class ParagraphTokenizer extends Tokenizer {

	protected ParagraphTokenizer(Version matchVersion, Reader input) {
		super(input);
	    charUtils = CharacterUtils.getInstance(matchVersion);
	}

	private int offset = 0, bufferIndex = 0, dataLen = 0, finalOffset = 0;
	private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
	private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
	private final CharacterUtils charUtils;
	private final CharacterBuffer ioBuffer = CharacterUtils.newCharacterBuffer(4096);

	@Override
	public final boolean incrementToken() throws IOException {
		clearAttributes();
		int length = 0;
		int start = -1; // this variable is always initialized
		int end = -1;
		char[] buffer = termAtt.buffer();
		int prevC = -1;
		while (true) {
			if (bufferIndex >= dataLen) {
				offset += dataLen;
				charUtils.fill(ioBuffer, input); // read supplementary char aware with CharacterUtils
				if (ioBuffer.getLength() == 0) {
					dataLen = 0; // so next offset += dataLen won't decrement offset
					if (length > 0) {
						break;
					} else {
						finalOffset = correctOffset(offset);
						return false;
					}
				}
				dataLen = ioBuffer.getLength();
				bufferIndex = 0;
			}
			// use CharacterUtils here to support < 3.1 UTF-16 code unit behavior if the char based methods are gone
			final int c = charUtils.codePointAt(ioBuffer.getBuffer(), bufferIndex, ioBuffer.getLength());
			final int charCount = Character.charCount(c);
			bufferIndex += charCount;

			if (c != '\n' || prevC != '\n') {
				if (length == 0) { // start of token
					assert start == -1;
					start = offset + bufferIndex - charCount;
					end = start;
				} else if (length >= buffer.length - 1) { // check if a supplementary could run out of bounds
					buffer = termAtt.resizeBuffer(2 + length); // make sure a supplementary fits in the buffer
				}
				end += charCount;
				length += Character.toChars(c, buffer, length);
			} else if (length > 0) // at non-Letter w/ chars
				break; // return 'em
			prevC = c;
		}

		termAtt.setLength(length);
		assert start != -1;
		offsetAtt.setOffset(correctOffset(start), finalOffset = correctOffset(end));
		return true;
	}

	@Override
	public final void end() throws IOException {
		super.end();
		offsetAtt.setOffset(finalOffset, finalOffset);
	}

	@Override
	public void reset() throws IOException {
		super.reset();
		bufferIndex = 0;
		offset = 0;
		dataLen = 0;
		finalOffset = 0;
		ioBuffer.reset(); // make sure to reset the IO buffer!!
	}
}

class SignatureFilter extends FilteringTokenFilter {

	private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

	public SignatureFilter(Version version, TokenStream in) {
		super(version, in);
	}

	@Override
	protected boolean accept() throws IOException {
		String str = termAtt.toString().trim();
		if (str.isEmpty()) {
			return false;
		}
		String lower = str.toLowerCase();
		if (lower.indexOf("signed-off-by:") != -1 || lower.indexOf("cc:") != -1 || lower.indexOf("git-svn-id:") != -1 || lower.indexOf("bitkeeper revision") != -1) {
			return false;
		}
		if ((lower.indexOf("commit") != -1 && lower.indexOf("upstream") != -1) || lower.indexOf("cherry pick") != -1) {
			return false;
		}
		return true;
	}
}
