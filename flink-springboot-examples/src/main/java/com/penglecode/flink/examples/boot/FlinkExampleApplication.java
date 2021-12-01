package com.penglecode.flink.examples.boot;

import com.penglecode.flink.BasePackage;
import com.penglecode.flink.common.util.JsonUtils;
import com.penglecode.flink.common.util.SpringUtils;
import com.penglecode.flink.examples.FlinkExample;
import com.penglecode.flink.examples.wordcount.WordCountExample;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.util.ClassUtils;

/**
 * Flink示例启动入口
 *
 * @author pengpeng
 * @version 1.0
 * @since 2021/11/29 23:12
 */
@SpringBootApplication(scanBasePackageClasses=BasePackage.class)
public class FlinkExampleApplication implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkExampleApplication.class);

    public static void main(String[] args) {
        //本例以非web方式(Servlet,Reactive)启动
        new SpringApplicationBuilder(FlinkExampleApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run(ApplicationArguments args) throws Exception {
        LOGGER.info("【ApplicationRunner】==> args = {}", JsonUtils.object2Json(args));
        ParameterTool parameterTool = ParameterTool.fromArgs(args.getSourceArgs());
        String flinkExampleClassName = parameterTool.get("flink.example.class", WordCountExample.class.getName());
        Class<? extends FlinkExample> flinkExampleClass = (Class<? extends FlinkExample>) ClassUtils.forName(flinkExampleClassName, ClassUtils.getDefaultClassLoader());
        SpringUtils.getBean(flinkExampleClass).run(args);
    }

}
