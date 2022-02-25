package com.info.infomover.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {
	public static String load(String path) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

			final StringBuilder sb = new StringBuilder();
			String s = null;
			String ls = "\r\n";

			boolean first = true;
			while ((s = reader.readLine()) != null) {
				/*
				 * If there's more than one line to be displayed, we need to add a newline to the StringBuilder. For the
				 * first line, we don't do so.
				 */
				if (!first) {
					sb.append(ls);
				}
				first = false;
				sb.append(s);
			}
			reader.close();
			String content = sb.toString();
			return content;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static List<String> readLines(String path) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

			final List<String> lines = new ArrayList<>();
			String s = null;

			boolean first = true;
			while ((s = reader.readLine()) != null) {
				lines.add(s);
			}
			reader.close();
			return lines;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static String save(String filePath, String content) {
		try {
			File file = new File(filePath);
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			writer.write(content);
			writer.close();
			return file.getAbsolutePath();
		} catch (Exception e) {
			throw new RuntimeException("save flow json to file failed", e);
		}
	}

	public static String loadFromClassPath(String path) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(FileUtil.class.getResourceAsStream(path)));

			final StringBuilder sb = new StringBuilder();
			String s = null;
			String ls = "\r\n";

			boolean first = true;
			while ((s = reader.readLine()) != null) {
				/*
				 * If there's more than one line to be displayed, we need to add a newline to the StringBuilder. For the
				 * first line, we don't do so.
				 */
				if (!first) {
					sb.append(ls);
				}
				first = false;
				sb.append(s);
			}
			reader.close();
			String content = sb.toString();
			return content;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
