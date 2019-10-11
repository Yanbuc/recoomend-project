package jdbc;

import conf.ConfigurationManager;
import constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

// 数据库的辅助助手
public class JDBCHelper {

    private  static JDBCHelper instance;
    static {
        try {
              String driverName= ConfigurationManager.getProperty(Constants.DRIVER_NAME);
              Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    // 连接池
    private LinkedList<Connection> pool=new LinkedList<Connection>();

    private JDBCHelper(){
      String url="";
      String user="";
      String password="";
      boolean local =ConfigurationManager.getBoolean(Constants.LOCAL);
      if(local){
          url=ConfigurationManager.getProperty(Constants.LOCAL_DB_URL);
          user=ConfigurationManager.getProperty(Constants.LOCAL_DB_USER);
          password=ConfigurationManager.getProperty(Constants.LOCAL_DB_PASSWORD);
      }else{
          url=ConfigurationManager.getProperty(Constants.PRODUCT_DB_URL);
          user=ConfigurationManager.getProperty(Constants.PRODUCT_DB_USER);
          password=ConfigurationManager.getProperty(Constants.PRODUCT_DB_PASSWORD);
      }
      for(int i=0;i<10;i++){
          try {
              Connection conn = DriverManager.getConnection(url,user,password);
              pool.push(conn);
          } catch (SQLException e) {
              e.printStackTrace();
          }
      }
    }
     // 获得实例
    public static JDBCHelper getInstance(){
        if(instance==null){
            synchronized (JDBCHelper.class){
                if(instance==null){
                    instance =new JDBCHelper();
                }
            }
        }
        return instance;
    }

    // 获得连接池之中的连接
     private synchronized Connection getConnnection(){
        while (pool.size()<1){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return pool.poll();
    }

    // 对数据库的插入 删除  更新
    public  int executeUpdate(String sql,Object[] params){
        Connection conn=getConnnection();
        PreparedStatement pre=null;
        int retn=0;
        try {
            while (conn==null){
                conn=getConnnection();
            }
            pre = conn.prepareStatement(sql);
            if(params!=null) {
                for (int i = 0; i < params.length; i++) {
                    pre.setObject(i + 1, params[i]);
                }
            }
           retn= pre.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn!=null){
                pool.push(conn);
            }
            if(pre!=null){
                try {
                    pre.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return retn;
    }


    // 批量实现插入 修改 删除
    public void executeBatch(String sql, List<Object[]> lists){
        // 在批量修改的地方 我的代码写的不对 在批量修改的时候 应该加上一个事务
        // 然后也不是简单地调用单个插入地API
         Connection conn= getConnnection();
         PreparedStatement preparedStatement =null;
        try {
            while (conn==null){
                conn=getConnnection();
            }
            preparedStatement=conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            if(lists!=null) {
                for (Object[] p : lists) {
                    for (int i = 0; i < p.length; i++) {
                        preparedStatement.setObject(i + 1, p[i]);
                    }
                    preparedStatement.addBatch();
                }
            }
            preparedStatement.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn!=null){
                pool.push(conn);
            }
            if(preparedStatement!=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    // 对数据库进行查询
    public void  executeQuery(String sql,Object[] params,QueryCallBack queryCallBack){
        Connection conn=getConnnection();
        PreparedStatement preparedStatement=null;
        try {
            while (conn==null){
                conn=getConnnection();
            }
             preparedStatement = conn.prepareStatement(sql);
             if(params!=null){
                 for(int i=0;i<params.length;i++){
                     preparedStatement.setObject(i+1,params[i]);
                 }
             }
            ResultSet resultSet = preparedStatement.executeQuery();
            queryCallBack.process(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn!=null){
                pool.push(conn);
            }
            if(preparedStatement!=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    // 这个接口 定义了怎么处理 查询出来的数据 在获得查询的数据的时候 在外面加上一个final 修饰的变量就是可以了。
    // 然后在里面调用
    public  static interface QueryCallBack{
          public void process(ResultSet resultSet) throws SQLException;
    }

}
