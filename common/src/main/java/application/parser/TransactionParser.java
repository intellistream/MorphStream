package application.parser;

import application.helper.parser.Parser;
import application.util.datatypes.StreamValues;
import com.google.common.collect.ImmutableList;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TransactionParser extends Parser {

	private static final long serialVersionUID = -4929179496810491167L;

	@Override
	public Object[] parse(char[] value) {
		LinkedList<char[]> ll = new LinkedList<>();
		int index = 0;
		int length = value.length;
		int cnt = 0;
		for (int c = 0; c < length; c++) {
			if (value[c] == ',') {
				if (cnt == 1) {
					int len = length - index;
					char[] word = new char[len];
					System.arraycopy(value, index, word, 0, len);
					ll.add(word);
					break;
				} else {
					int len = c - index;
					char[] word = new char[len];
					System.arraycopy(value, index, word, 0, len);
					ll.add(word);
					cnt++;
					index = c + 1;
				}
			}
		}
		return ll.toArray();
//		String[] items = new String(value).split(",", 2);//return ImmutableList.of(new StreamValues(items[0], items[1]));
//		return new Object[]{items[0].toCharArray(), items[1].toCharArray()};
	}

	@Override
	public List<StreamValues> parse(String value) {
		String[] items = value.split(",", 2);//return ImmutableList.of(new StreamValues(items[0], items[1]));
		return ImmutableList.of(new StreamValues(items[0], items[1]));
	}
}
