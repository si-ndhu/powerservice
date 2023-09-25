import java.sql.*;
import java.util.concurrent.Executor;

public class DbConnection {
    Connection makeConnection() {
        Connection connect = null;
        Statement statement = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (Exception ex) {
            System.out.println("Unable to connect");
        }
        try {
            connect = DriverManager.getConnection("jdbc:mysql://db.cs.dal.ca:3306?serverTimezone=UTC&useSSL=false", "ssb", "92BzqG9WEnKECgpWsAia3GsEZ");
            statement = connect.createStatement();
            statement.execute("use ssb;");
        } catch (Exception e){
            System.out.println("connection failed "+ e);
        }
        return connect;
    }
}