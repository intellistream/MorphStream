package intellistream.morphstream.util;

import org.apache.commons.lang.StringUtils;

public class PrintTable {
    public static void printTable(String[] headers, String[][] data) {
        for (String header : headers) {
            System.out.print(padStringWithoutBordered(header, 20) + "\t");
        }
        System.out.println();

        for (String[] row : data) {
            for (String value : row) {
                System.out.print(padStringWithoutBordered(value, 20) + "\t");
            }
            System.out.println();
        }
    }

    private static String padStringWithoutBordered(String s, int length) {
        return String.format("%-" + length + "s", s);
    }
    public static void printBorderedTable(String[] headers, String[][] data) {
        printHorizontalLine(headers.length);

        printRow(headers);

        printHorizontalLine(headers.length);

        for (String[] row : data) {
            printRow(row);
        }

        printHorizontalLine(headers.length);
    }

    private static void printHorizontalLine(int columns) {
        for (int i = 0; i < columns; i++) {
            System.out.print("+-------------------");
        }
        System.out.println("+");
    }

    private static void printRow(String[] values) {
        for (String value : values) {
            System.out.print("| " + padString(value, 18) + " ");
        }
        System.out.println("|");
    }

    private static String padString(String s, int length) {
        return String.format("%-" + length + "s", s);
    }
    public static void printFancyBorderedTable(String[] headers, String[][] data) {
        // 打印上边框
        printFancyHorizontalLine(headers.length);

        // 打印表头
        printFancyRow(headers);

        // 打印中间分隔线
        printFancyHorizontalLine(headers.length);

        // 打印数据
        for (String[] row : data) {
            printFancyRow(row);
        }

        // 打印下边框
        printFancyHorizontalLine(headers.length);
    }

    // 打印水平线
    private static void printFancyHorizontalLine(int columns) {
        for (int i = 0; i < columns; i++) {
            System.out.print("+-----------");
        }
        System.out.println("+");
    }

    // 打印带边框的表格的一行
    private static void printFancyRow(String[] values) {
        for (String value : values) {
            System.out.print("| " + padString(value, 11) + " ");
        }
        System.out.println("|");
    }
    public static void printAlignedBorderedTable(String[] headers, String[][] data) {
        // 计算每列的最大宽度
        int[] columnWidths = calculateColumnWidths(headers, data);

        // 打印上边框
        printAlignedHorizontalLine(columnWidths);

        // 打印表头
        printAlignedRow(headers, columnWidths);

        // 打印中间分隔线
        printAlignedHorizontalLine(columnWidths);

        // 打印数据
        for (String[] row : data) {
            printAlignedRow(row, columnWidths);
        }

        // 打印下边框
        printAlignedHorizontalLine(columnWidths);
    }

    // 打印水平线
    private static void printAlignedHorizontalLine(int[] columnWidths) {
        for (int width : columnWidths) {
            System.out.print("+" + StringUtils.repeat("-", width + 2));
        }
        System.out.println("+");
    }

    // 打印带边框的表格的一行
    private static void printAlignedRow(String[] values, int[] columnWidths) {
        for (int i = 0; i < values.length; i++) {
            System.out.printf("| %-" + columnWidths[i] + "s ", values[i]);
        }
        System.out.println("|");
    }

    // 计算每列的最大宽度
    private static int[] calculateColumnWidths(String[] headers, String[][] data) {
        int[] columnWidths = new int[headers.length];

        // 初始化列宽度为表头长度
        for (int i = 0; i < headers.length; i++) {
            columnWidths[i] = headers[i].length();
        }

        // 更新列宽度为数据最大长度
        for (String[] row : data) {
            for (int i = 0; i < row.length; i++) {
                columnWidths[i] = Math.max(columnWidths[i], row[i].length());
            }
        }

        return columnWidths;
    }
}
