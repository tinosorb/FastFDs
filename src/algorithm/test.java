//package algorithm;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ListIterator;

public class test {
	private static Connection connection;
	private static Integer numberOfAttributes;
	private static String table;
	private static long query_time;
	private static Vector<String> attrStr;
	private static Statement st;

	
	public static void init(String[] cmdinput){

//		numberOfAttributes = Integer.parseInt(cmdinput[1]);                 
 		table = cmdinput[0];
		attrStr = new Vector<String>(0);
//		primaryKey = 0;
//		lattice = new Vector<Integer>(0);
//		depen_set = new Hashtable<Integer,HashSet<Integer>>(0);
//		time_ref = new Hashtable<Integer, Integer>(0);
//		check_subset_time = new Integer(0);
//		add_to_exist_time = new Integer(0);
		query_time = new Integer(0);
//		query_times = new Integer(0);

		try {
 
			Class.forName("org.postgresql.Driver");
 
		} catch (ClassNotFoundException e) {
 
			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return;
 
		}

		try {
			connection = DriverManager.getConnection(
					"jdbc:postgresql://localhost:5432/postgres", "postgres",
					"");
//			connection = DriverManager.getConnection(
//					"jdbc:postgresql://127.0.0.1:5432/postgres?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory", "postgres",
//					"");
 			connection.setAutoCommit(false);
 			st = connection.createStatement();

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
	}

	
	private static void get_diffset(){
		try {
			
//			String get_column_name = "select column_name"
//					+ "from information_schema.columns"
//					+ "where table_name='"
//					+ table
//					+ "';
			
			String add_id = "alter table " + table + " add id SERIAL";
			 
			String inner_join = "CREATE VIEW joined AS "
					+ "SELECT row_number() OVER() AS id, t1.a AS A1, t2.a AS A2, t1.b AS B1, t2.b AS B2, t1.c AS C1, t2.c AS C2, t1.d AS D1, t2.d AS D2, "
					+ "t1.e AS E1, t2.e AS E2, t1.f AS F1, t2.f AS F2 "
					+ "FROM fastfd t1 "
					+ "INNER JOIN fastfd t2 "
					+ "ON t1.id <t2.id";
			
			String diff_set = "CREATE VIEW diffset AS "
					+ "SELECT id, 'a' AS Diff "
					+ "FROM ( "
					+ "SELECT id, a1, a2 FROM joined "
					+ "WHERE a1 <> a2 "
					+ ") AS Diffs "
					+ "UNION "
					+ "SELECT id, 'b' AS Diff "
					+ "FROM ( "
					+ "SELECT id, b1, b2 FROM joined "
					+ "WHERE b1 <> b2 "
					+ ") AS Diffs "
					+ "UNION "
					+ "SELECT id, 'c' AS Diff "
					+ "FROM ( "
					+ "SELECT id, c1, c2 FROM joined "
					+ "WHERE c1 <> c2 "
					+ ") AS Diffs "
					+ "UNION "
					+ "SELECT id, 'd' AS Diff "
					+ "FROM ( "
					+ "SELECT id, d1, d2 FROM joined "
					+ "WHERE d1 <> d2 "
					+ ") AS Diffs "
					+ "UNION "
					+ "SELECT id, 'e' AS Diff "
					+ "FROM ( "
					+ "SELECT id, e1, e2 FROM joined "
					+ "WHERE e1 <> e2 "
					+ ") AS Diffs "
					+ "UNION "
					+ "SELECT id, 'f' AS Diff "
					+ "FROM ( "
					+ "SELECT id, f1, f2 FROM joined " 
					+ "WHERE f1 <> f2 "
					+ ") AS Diffs";
					
			String diffset_output = "CREATE VIEW diffset_output AS "
					+ "SELECT id, array_agg(diff) AS DifferenceSets "
					+ "FROM diffset "
					+ "GROUP BY id";
			
			String receive_diffset = "SELECT * FROM diffset_output";
			
			long start1 = System.currentTimeMillis();
			Statement st1 = connection.createStatement();
			st1.executeUpdate(add_id);
//			System.out.println(add_id);
			System.out.println("ID column added");
			query_time = query_time + (System.currentTimeMillis() - start1);
			System.out.println("Adding key: " + query_time);
			
			
			long start2 = System.currentTimeMillis();
			Statement st2 = connection.createStatement();
			st2.executeUpdate(inner_join);
//			System.out.println(inner_join);
			System.out.println("Joined view created");
			query_time = query_time + (System.currentTimeMillis() - start2);
			System.out.println("Creating self-joined view: " + query_time);

						
			long start3 = System.currentTimeMillis();
			Statement st3 = connection.createStatement();
			st3.executeUpdate(diff_set);
//			System.out.println(diff_set);
			System.out.println("Differences spotted");
			query_time = query_time + (System.currentTimeMillis() - start3);
			System.out.println("Generating diffs: " + query_time);

			
			long start4 = System.currentTimeMillis();
			Statement st4 = connection.createStatement();
			st4.executeUpdate(diffset_output);
//			System.out.println(diffset_output);
			System.out.println("Diffset view created");
			query_time = query_time + (System.currentTimeMillis() - start4);
			System.out.println("Aggregating diffsets: " + query_time);

			
			long start5 = System.currentTimeMillis();
			Statement st5 = connection.createStatement();
			ResultSet rs = st5.executeQuery(receive_diffset);
			System.out.println("Diffset received!");
			query_time = query_time + (System.currentTimeMillis() - start5);
			System.out.println("Receiving diffsets: " + query_time);

			
			while(rs.next()){
			
				Integer first = rs.getInt(1);
			    String second = rs.getString(2);
				System.out.println(first + " " + second);
				
			}
		
                        rs.close();
                        st1.close();
                        st2.close();  
                        st3.close();
	                st4.close();
                        st5.close();

		} catch(SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
	}
	
	
	public static void main(String[] argv) {
  		long total = System.currentTimeMillis();

		init(argv);

		get_diffset();
		
		System.out.println("total time: " + (System.currentTimeMillis() - total));
		System.out.println("query_time: " + query_time);
	}
	
}
