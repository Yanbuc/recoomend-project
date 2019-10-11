package conf;

import constant.Constants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

// 配置类 从配置文件之中读取配置
public class ConfigurationManager {
    private static Properties properties = new Properties();

    static { // 静态初始化
        InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream(Constants.CONFIG_FILE_NAME);
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static String getProperty(String key){
        return properties.getProperty(key);
    }

    public static boolean getBoolean(String key){
         return  Boolean.valueOf(getProperty(key));
    }
}
