import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.*;

public class dbpuller {
	private static Connection connection;
	private static Integer primaryKey;
	private static String table;
	private static long query_time;
	
	private static void get_diffset(){
		try {
			
			String get_column_name = "select column_name"
					+ "from information_schema.columns"
					+ "where table_name='"
					+ table
					+ "';";
					
			String add_id = "alter table"
					+ table
					+ "add id SERIAL;";
			 
			String inner_join = "create table joined as"
					+ "select t1.";
					
			String str = "";
			
			long start = System.currentTimeMillis();
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery(str);
			query_time = query_time + (System.currentTimeMillis() - start);
			
			
			
		} catch(SQLException e) {
			
		}
	}
	
}

