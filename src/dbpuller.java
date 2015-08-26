import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.*;
import java.util.Vector;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ListIterator;

public class dbpuller {
	private static Connection connection;
	private static Integer primaryKey;
	private static String table;
	private static long query_time;
	
	
	private static void get_diffset(){
		try {
			
//			String get_column_name = "select column_name"
//					+ "from information_schema.columns"
//					+ "where table_name='"
//					+ table
//					+ "';
			
			String add_id = "alter table"
					+ table
					+ "add id SERIAL";
			 
			String inner_join = "CREATE TABLE joined AS"
					+ "SELECT t1.a AS A1, t2.a AS A2, t1.b AS B1, t2.b AS B2, t1.c AS C1, t2.c AS C2, t1.d AS D1, t2.d AS D2,"
					+ "t1.e AS E1, t2.e AS E2, t1.f AS F1, t2.f AS F2"
					+ "FROM fastfd t1"
					+ "INNER JOIN fastfd t2"
					+ "ON t1.id <t2.id";
			
			String diff_set = "CREATE TABLE diffset AS"
					+ "SELECT id, 'a' AS Diff"
					+ "FROM ("
					+ "SELECT id, a1, a2 FROM joined"
					+ "WHERE a1 <> a2"
					+ ") AS Diffs"
					+ "UNION"
					+ "SELECT id, 'b' AS Diff"
					+ "FROM ("
					+ "SELECT id, b1, b2 FROM joined"
					+ "WHERE b1 <> b2"
					+ ") AS Diffs"
					+ "UNION"
					+ "SELECT id, 'c' AS Diff"
					+ "FROM ("
					+ "SELECT id, c1, c2 FROM joined"
					+ "WHERE c1 <> c2"
					+ ") AS Diffs"
					+ "UNION"
					+ "SELECT id, 'd' AS Diff"
					+ "FROM ("
					+ "SELECT id, d1, d2 FROM joined"
					+ "WHERE d1 <> d2"
					+ ") AS Diffs"
					+ "UNION"
					+ "SELECT id, 'e' AS Diff"
					+ "FROM ("
					+ "SELECT id, e1, e2 FROM joined"
					+ "WHERE e1 <> e2"
					+ ") AS Diffs"
					+ "UNION"
					+ "SELECT id, 'f' AS Diff"
					+ "FROM ("
					+ "SELECT id, f1, f2 FROM joined"
					+ "WHERE f1 <> f2"
					+ ") AS Diffs";
					
			String diffset_output = "CREATE TABLE diffset_output AS"
					+ "SELECT id, array_agg(diff) AS DifferenceSets"
					+ "FROM diffset"
					+ "GROUP BY id";
			
			String str = "";
			
			long start = System.currentTimeMillis();
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery(str);
			query_time = query_time + (System.currentTimeMillis() - start);
			
			
		} catch(SQLException e) {
			System.out.println("Connection Failed! Check output console");
			return;
		}
	}


	
	
	
	
	
	
	
	
	
	
}

