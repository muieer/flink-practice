package org.muieer.flink_practice.java.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CommandLineArgumentToFlinkConfiguration {

    public static void main(String[] args) {
        System.out.println(commandLineArgumentToFlinkConfiguration("-cmd1 arg1 -cmd2 arg2 -cmd3 -cmd4"));
        System.out.println(commandLineArgumentToFlinkConfigurationByParameterTool(args));
    }

    /*
    * {@link org.apache.flink.api.java.utils.ParameterTool} 是 Flink 提供的参数解析工具，文档是
    * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/application_parameters/">
    * Handling Application Parameters</a>
    * {@link org.apache.flink.api.java.utils.MultipleParameterTool} 能解析多个参数
    * */
    public static ParameterTool parseCommandLineArgumentByParameterTool(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        System.out.println(parameterTool.toMap());
        return parameterTool;
    }

    public static Configuration commandLineArgumentToFlinkConfigurationByParameterTool(String[] mainMethodArgs) {
        return ParameterTool.fromArgs(mainMethodArgs).getConfiguration();
    }

    public static Configuration commandLineArgumentToFlinkConfiguration(String[] mainMethodArgs) {
        String originCommandLineArgument = StringUtils.join(mainMethodArgs, " ");
        return commandLineArgumentToFlinkConfiguration(originCommandLineArgument);
    }

    /*
    * 命令行解析做的不完善，需要严格遵守输入规范，否则会出错
    * 输入范例：-cmd1 arg1 -cmd2 arg2 -cmd3 -cmd4
    * 规则：cmd以-开头,cmd 与 arg 或 cmd 空格分割
    * */
    public static Configuration commandLineArgumentToFlinkConfiguration(String originCommandLineArgument) {
        if (!originCommandLineArgument.startsWith("-")) {
            throw new IllegalArgumentException("originCommandLineArgument should start with -");
        }
        String[] kvArray = originCommandLineArgument.substring(1).split("\\s-");
        Map<String, String> kvMap = new HashMap<>();
        Arrays.stream(kvArray).forEach(kv -> {
            String[] keyAndValue = kv.split("\\s");
            if (keyAndValue.length == 1) {
                kvMap.put(keyAndValue[0], "no_arg");
            } else if (keyAndValue.length == 2) {
                kvMap.put(keyAndValue[0], keyAndValue[1]);
            } else {
                throw new IllegalArgumentException("CommandLine Argument is invalid: " + kv + ", 输入范例:-cmd1 arg1 -cmd2 arg2 -cmd3 -cmd4");
            }
        });
        return Configuration.fromMap(kvMap);
    }
}
