package dao;

import daomain.RecUser;
import jdbc.JDBCHelper;
import scala.Tuple1;
import scala.Tuple2;

import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RecUserImpl {


    private String getDate(){
        Date date=new Date(System.currentTimeMillis());
        SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd");
        String day = format.format(date);
        return day;
    }

    public void insert(List<RecUser> data){
        String sql="insert into rec_user values(?,?,?)";
        String day=getDate();
        JDBCHelper con=JDBCHelper.getInstance();
        List<Object[]> params=new ArrayList<Object[]>();
        for(RecUser d:data){
            Object[] p={d.getUserId(),d.getRecInfo(),day};
            params.add(p);
        }
        con.executeBatch(sql,params);
    }
    // 更新
    public void update(List<RecUser> data){
        String day=getDate();
        String sql="update rec_user set res=? , day= ?  where user_id= ? ";
        List<Object[]> params=new ArrayList<Object[]>();
        JDBCHelper con=JDBCHelper.getInstance();
        for(RecUser d:data){
            Object[] p={d.getRecInfo(),day,d.getUserId()};
            params.add(p);
        }
        con.executeBatch(sql,params);
    }

    public void insertAndUpdate(List<RecUser> data){
        Map<Integer, Tuple2<String,String>> one=new HashMap<Integer, Tuple2<String, String>>();
        JDBCHelper con=JDBCHelper.getInstance();
        StringBuffer sb=new StringBuffer();
        sb.append("(");
        for(RecUser recUser:data){
            sb.append(recUser.getUserId()+",");
        }
        String userList=sb.substring(0,sb.length()-1)+" )";
        String sql ="select user_id,day from rec_user where user_id in "+userList;
        Map<Integer, Tuple2<String,String>> userIds=new HashMap<Integer, Tuple2<String, String>>();
        con.executeQuery(sql, null, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet resultSet) throws SQLException {
                 while (resultSet.next()){
                     userIds.put(resultSet.getInt(1),new Tuple2<String, String>(resultSet.getString(2),"b"));
                 }
            }
        });
        if(userIds.size()==0||userIds==null){ // 用户的数据都是存在的。
            insert(data);
        }else{
            List<RecUser> update=new ArrayList<RecUser>();
            List<RecUser> insert=new ArrayList<RecUser>();
            String day=getDate();
            Tuple2<String,String> t=new Tuple2<String, String>("a","a");
            for(RecUser recUser:data){
                if(userIds.getOrDefault(recUser.getUserId(),new Tuple2<String, String>("a","a"))._2.equals("b")){
                    if(!day.equals(userIds.get(recUser.getUserId())._1)){
                        update.add(recUser);
                    }
                }else{
                    insert.add(recUser);
                }
            }
            // 这里曾经直接将 data 这个list 进行修改 然后就出现了list 并发错误。
            insert(insert);
            update(update);
        }
    }
}
