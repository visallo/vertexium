package org.vertexium.elasticsearch7.lucene;

import org.vertexium.VertexiumException;

import java.util.Locale;

public class EscapeQuerySyntax {
    private static final String[] escapableTermExtraFirstChars = {"+", "-", "@"};

    private static final String[] escapableTermChars = {"\"", "<", ">", "=",
        "!", "(", ")", "^", "[", "{", ":", "]", "}", "~", "/"};

    // TODO: check what to do with these "*", "?", "\\"
    private static final String[] escapableQuotedChars = {"\""};
    private static final String[] escapableWhiteChars = {" ", "\t", "\n", "\r",
        "\f", "\b", "\u3000"};
    private static final String[] escapableWordTokens = {"AND", "OR", "NOT",
        "TO", "WITHIN", "SENTENCE", "PARAGRAPH", "INORDER"};

    private static CharSequence escapeChar(CharSequence str, Locale locale) {
        if (str == null || str.length() == 0) {
            return str;
        }

        CharSequence buffer = str;

        // regular escapable Char for terms
        for (String escapableTermChar : escapableTermChars) {
            buffer = replaceIgnoreCase(buffer, escapableTermChar.toLowerCase(locale), "\\", locale);
        }

        // First Character of a term as more escaping chars
        for (String escapableTermExtraFirstChar : escapableTermExtraFirstChars) {
            if (buffer.charAt(0) == escapableTermExtraFirstChar.charAt(0)) {
                buffer = "\\" + buffer.charAt(0) + buffer.subSequence(1, buffer.length());
                break;
            }
        }

        return buffer;
    }

    private CharSequence escapeQuoted(CharSequence str, Locale locale) {
        if (str == null || str.length() == 0) {
            return str;
        }

        CharSequence buffer = str;

        for (int i = 0; i < escapableQuotedChars.length; i++) {
            buffer = replaceIgnoreCase(buffer, escapableTermChars[i].toLowerCase(locale), "\\", locale);
        }
        return buffer;
    }

    public static CharSequence escapeTerm(CharSequence term, Locale locale) {
        if (term == null) {
            return term;
        }

        // Escape single Chars
        term = escapeChar(term, locale);
        term = escapeWhiteChar(term, locale);

        // Escape Parser Words
        for (String escapableWordToken : escapableWordTokens) {
            if (escapableWordToken.equalsIgnoreCase(term.toString())) {
                return "\\" + term;
            }
        }
        return term;
    }

    /**
     * replace with ignore case
     *
     * @param string     string to get replaced
     * @param sequence1  the old character sequence in lowercase
     * @param escapeChar the new character to prefix sequence1 in return string.
     * @return the new String
     */
    private static CharSequence replaceIgnoreCase(CharSequence string, CharSequence sequence1, CharSequence escapeChar, Locale locale) {
        if (escapeChar == null || sequence1 == null || string == null) {
            throw new NullPointerException();
        }

        // empty string case
        int count = string.length();
        int sequence1Length = sequence1.length();
        if (sequence1Length == 0) {
            StringBuilder result = new StringBuilder((count + 1)
                * escapeChar.length());
            result.append(escapeChar);
            for (int i = 0; i < count; i++) {
                result.append(string.charAt(i));
                result.append(escapeChar);
            }
            return result.toString();
        }

        // normal case
        StringBuilder result = new StringBuilder();
        char first = sequence1.charAt(0);
        int start = 0, copyStart = 0, firstIndex;
        while (start < count) {
            if ((firstIndex = string.toString().toLowerCase(locale).indexOf(first, start)) == -1) {
                break;
            }
            boolean found = true;
            if (sequence1.length() > 1) {
                if (firstIndex + sequence1Length > count) {
                    break;
                }
                for (int i = 1; i < sequence1Length; i++) {
                    if (string.toString().toLowerCase(locale).charAt(firstIndex + i) != sequence1
                        .charAt(i)) {
                        found = false;
                        break;
                    }
                }
            }
            if (found) {
                result.append(string.toString(), copyStart, firstIndex);
                result.append(escapeChar);
                result.append(string.toString(), firstIndex, firstIndex + sequence1Length);
                copyStart = start = firstIndex + sequence1Length;
            } else {
                start = firstIndex + 1;
            }
        }
        if (result.length() == 0 && copyStart == 0) {
            return string;
        }
        result.append(string.toString().substring(copyStart));
        return result.toString();
    }

    /**
     * escape all tokens that are part of the parser syntax on a given string
     *
     * @param str    string to get replaced
     * @param locale locale to be used when performing string compares
     * @return the new String
     */
    private static CharSequence escapeWhiteChar(CharSequence str, Locale locale) {
        if (str == null || str.length() == 0) {
            return str;
        }

        CharSequence buffer = str;

        for (String escapableWhiteChar : escapableWhiteChars) {
            buffer = replaceIgnoreCase(buffer, escapableWhiteChar.toLowerCase(locale), "\\", locale);
        }
        return buffer;
    }

    /**
     * Returns a String where the escape char has been removed, or kept only once
     * if there was a double escape.
     * <p>
     * Supports escaped unicode characters, e. g. translates <code>A</code> to
     * <code>A</code>.
     */
    public static String discardEscapeChar(CharSequence input) {
        // Create char array to hold unescaped char sequence
        char[] output = new char[input.length()];

        // The length of the output can be less than the input
        // due to discarded escape chars. This variable holds
        // the actual length of the output
        int length = 0;

        // We remember whether the last processed character was
        // an escape character
        boolean lastCharWasEscapeChar = false;

        // The multiplier the current unicode digit must be multiplied with.
        // E. g. the first digit must be multiplied with 16^3, the second with
        // 16^2...
        int codePointMultiplier = 0;

        // Used to calculate the codepoint of the escaped unicode character
        int codePoint = 0;

        for (int i = 0; i < input.length(); i++) {
            char curChar = input.charAt(i);
            if (codePointMultiplier > 0) {
                codePoint += hexToInt(curChar) * codePointMultiplier;
                codePointMultiplier >>>= 4;
                if (codePointMultiplier == 0) {
                    output[length++] = (char) codePoint;
                    codePoint = 0;
                }
            } else if (lastCharWasEscapeChar) {
                if (curChar == 'u') {
                    // found an escaped unicode character
                    codePointMultiplier = 16 * 16 * 16;
                } else {
                    // this character was escaped
                    output[length] = curChar;
                    length++;
                }
                lastCharWasEscapeChar = false;
            } else {
                if (curChar == '\\') {
                    lastCharWasEscapeChar = true;
                } else {
                    output[length] = curChar;
                    length++;
                }
            }
        }

        if (codePointMultiplier > 0) {
            throw new VertexiumException("INVALID_SYNTAX_ESCAPE_UNICODE_TRUNCATION");
        }

        if (lastCharWasEscapeChar) {
            throw new VertexiumException("INVALID_SYNTAX_ESCAPE_CHARACTER");
        }

        return new String(output, 0, length);
    }

    /**
     * Returns the numeric value of the hexadecimal character
     */
    private static int hexToInt(char c) {
        if ('0' <= c && c <= '9') {
            return c - '0';
        } else if ('a' <= c && c <= 'f') {
            return c - 'a' + 10;
        } else if ('A' <= c && c <= 'F') {
            return c - 'A' + 10;
        } else {
            throw new VertexiumException("INVALID_SYNTAX_ESCAPE_NONE_HEX_UNICODE: " + c);
        }
    }
}
