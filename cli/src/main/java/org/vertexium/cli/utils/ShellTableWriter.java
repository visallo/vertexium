package org.vertexium.cli.utils;

import com.google.common.base.Strings;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ShellTableWriter {
    private static final Pattern GROOVY_FORMATTING_PATTERN = Pattern.compile("\\@\\|(.*?) (.*?)\\|\\@");

    public static String tableToString(List<List<String>> table) {
        int maxColumnCount = table.stream().map(List::size).max(Integer::compareTo).orElse(0);
        int[] columnWidths = new int[maxColumnCount];
        table.forEach(row -> {
            for (int i = 0; i < row.size(); i++) {
                String column = row.get(i);
                column = cleanTableColumnValue(column);
                columnWidths[i] = Math.max(columnWidths[i], column.length());
            }
        });

        StringBuilder results = new StringBuilder();
        results.append(seperatorRow(columnWidths)).append("\n");
        results.append(rowToString(table.get(0), columnWidths)).append("\n");
        results.append(seperatorRow(columnWidths)).append("\n");
        results.append(
            table.stream()
                .skip(1)
                .map(row -> rowToString(row, columnWidths))
                .collect(Collectors.joining("\n"))
        ).append("\n");
        results.append(seperatorRow(columnWidths)).append("\n");
        return results.toString();
    }

    private static String seperatorRow(int[] columnWidths) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < columnWidths.length; i++) {
            if (i == 0) {
                result.append("+-");
            } else {
                result.append("-");
            }
            int columnWidth = columnWidths[i];
            result.append(Strings.padEnd("", columnWidth, '-'));
            result.append("-+");
        }
        return result.toString();
    }

    private static String rowToString(List<String> row, int[] columnWidths) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < row.size(); i++) {
            if (i == 0) {
                result.append("| ");
            }
            String column = row.get(i);
            int columnWidth = columnWidths[i];
            column = cleanTableColumnValue(column);
            result.append(Strings.padEnd(column, columnWidth, ' '));
            result.append(" | ");
        }
        return result.toString();
    }

    private static String cleanTableColumnValue(String column) {
        if (column == null) {
            column = "<null>";
        } else {
            Matcher m = GROOVY_FORMATTING_PATTERN.matcher(column);
            if (m.find()) {
                StringBuffer sb = new StringBuffer();
                do {
                    m.appendReplacement(sb, m.group(2));
                } while (m.find());
                m.appendTail(sb);
                column = sb.toString();
            }
        }
        return column;
    }
}
