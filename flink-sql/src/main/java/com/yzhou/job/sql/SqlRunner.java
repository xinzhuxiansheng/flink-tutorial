package com.yzhou.job.sql;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class SqlRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SqlRunner.class);

    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";

    private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";

    public static void main(String[] args) throws Exception {
        String scriptFilePath = "/Users/a/Code/Java/flink-tutorial/flink-sql/src/main/resources/simple.sql";
        if (args.length == 1) {
            scriptFilePath = args[0];
        }

        String script = FileUtils.readFileUtf8(new File(scriptFilePath));
        List<String> statements = parseStatements(script);

        //建立Stream环境，设置并行度为1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));
        //建立Table环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        TableResult tableResult = null;
        for (String statement : statements) {
            LOG.info("Executing:\n{}", statement);
            System.out.println("Executing: " + statement);
            tableResult = tableEnvironment.executeSql(statement);
        }
//        ** executeSql是一个异步的接口，在idea里面跑的话，直接就结束了，需要手动拿到那个executeSql的返回的TableResult
        //tableResult.getJobClient().get().getJobExecutionResult().get();
        tableResult.print();
    }

    public static List<String> parseStatements(String script) {
        String formatted = formatSqlFile(script).replaceAll(COMMENT_PATTERN, "");
        List<String> statements = new ArrayList<String>();

        StringBuilder current = null;
        boolean statementSet = false;
        for (String line : formatted.split("\n")) {
            String trimmed = line.trim();
            if (trimmed == null || trimmed.length() < 1) {
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
            }
            if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
                statementSet = true;
            }
            current.append(trimmed);
            current.append("\n");
            if (trimmed.endsWith(STATEMENT_DELIMITER)) {
                if (!statementSet || trimmed.equals("END;")) {
                    // SQL语句不能以分号结尾
                    statements.add(current.toString().replace(";", ""));
                    current = null;
                    statementSet = false;
                }
            }
        }
        return statements;
    }

    public static String formatSqlFile(String content) {
        String trimmed = content.trim();
        StringBuilder formatted = new StringBuilder();
        formatted.append(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(STATEMENT_DELIMITER);
        }
        formatted.append(LINE_DELIMITER);
        return formatted.toString();
    }

}
