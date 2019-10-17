package org.vertexium.accumulo.iterator.util;

import org.vertexium.accumulo.iterator.model.VertexiumAccumuloIteratorException;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.regex.Pattern;

public class MultiFieldStringEncoder {
    private static final String ENC = StandardCharsets.UTF_8.toString();
    private final String separator;
    private final String separatorRegex;
    private final Integer expectedPartCount;

    public MultiFieldStringEncoder(String separator, Integer expectedPartCount) {
        this.separator = separator;
        this.separatorRegex = Pattern.quote(separator);
        this.expectedPartCount = expectedPartCount;
    }

    public String[] decode(String str) {
        String[] parts = str.split(separatorRegex);
        if (expectedPartCount != null && parts.length != expectedPartCount) {
            throw new VertexiumAccumuloIteratorException(String.format("Expected %d parts found %d in string \"%s\" while looking for \"%s\"", expectedPartCount, parts.length, str, separator));
        }
        for (int i = 0; i < parts.length; i++) {
            parts[i] = decodeField(parts[i]);
        }
        return parts;
    }

    private String decodeField(String field) {
        try {
            return URLDecoder.decode(field, ENC);
        } catch (UnsupportedEncodingException e) {
            throw new VertexiumAccumuloIteratorException("Bad encoder", e);
        }
    }

    public static ZonedDateTime timestampFromString(String str, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(str, 16)), zoneId);
    }

    public static String timestampToString(long timestamp) {
        return String.format("%08x", timestamp);
    }

    public String encode(String... fields) {
        if (expectedPartCount != null && fields.length != expectedPartCount) {
            throw new VertexiumAccumuloIteratorException(String.format("Expected %d parts found %d", expectedPartCount, fields.length));
        }
        StringBuilder result = new StringBuilder();
        for (String field : fields) {
            if (result.length() > 0) {
                result.append(separator);
            }
            result.append(encodeField(field));
        }
        return result.toString();
    }

    private String encodeField(String field) {
        try {
            return URLEncoder.encode(field, ENC);
        } catch (UnsupportedEncodingException e) {
            throw new VertexiumAccumuloIteratorException("Bad encoder", e);
        }
    }
}
