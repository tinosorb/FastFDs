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

public class tane_pg {

	private static Vector<String> attrStr;
	private static Vector<Integer> lattice;
	private static Hashtable<Integer,HashSet<Integer>> depen_set;
	private static Hashtable<Integer, Integer> time_ref;

	private static Integer primaryKey;
	private static Connection connection;
	private static Integer attr_num;
	private static String table;
	private static long check_subset_time;
	private static long add_to_exist_time;
	private static long query_time;
	private static long query_times;
	private static long statement_time;
	private static long bit_related;
	private static Statement st;
	private static long cfd;



	private static void first_level(){

		try {
			
			for (int i = 0; i < attr_num; i++) {
				String str = "select count("
					+	attrStr.elementAt(i)
					+	"),count(distinct("
					+	attrStr.elementAt(i)
					+	")) from "
					+ 	table;

				long start = System.currentTimeMillis();
				Statement st = connection.createStatement();
				ResultSet rs = st.executeQuery(str);
				query_time = query_time + (System.currentTimeMillis() - start);
				if(rs.next()){

					Integer first  = Integer.parseInt(rs.getString(1));
					Integer second = Integer.parseInt(rs.getString(2));

					if (first.equals(second)) {
						primaryKey = (primaryKey | (1<<i));
						System.out.println(attrStr.elementAt(i) 
							+ " (Primary Key)");
					}else{
						lattice.add((1<<i));
					}
				}else{
					System.out.println("ResultSet is empty--first level");
				}
				rs.close();
				st.close();
			}
 
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
 
		}

	}

	public static Boolean check_subset(Vector<Integer> set_pos, Integer rhs, Integer bits){

		Integer bits_rhs = bits-(1<<(set_pos.elementAt(rhs)));
		if( ( bits_rhs & primaryKey) != 0) return true;

		HashSet<Integer> rel = depen_set.get(set_pos.elementAt(rhs));		
		if(rel!=null){
			for (Integer existed: rel) {
				if( (existed & bits_rhs) == existed) {
					//have subset form the dependency
					return true;
				}
			}
		}else{
			return false;
		}

		return false;
	}


	public static void add_to_exist(Vector<Integer> set_pos, Integer rhs, Integer bits){

		Integer bits_rhs = bits-(1<<(set_pos.elementAt(rhs)));
		HashSet<Integer> rel = depen_set.get(set_pos.elementAt(rhs));
		if(rel!=null){
			rel.add(bits_rhs);
		}else{
			rel = new HashSet<Integer>(0);
			rel.add(bits_rhs);
			depen_set.put(set_pos.elementAt(rhs), rel);
		}

	}

	public static void print_depen(Vector<String> set_str, String s){
		String to_print = "";
		for (String val: set_str) {
			if(val != s){
				if(to_print!=""){
					to_print = to_print + " " + val;
				}else{
					to_print = val;
				}
			}
		}
		to_print = to_print + " -> " + s;
		System.out.println(to_print);
	}

	public static void add_time_ref(Vector<Integer> set_pos, Integer rhs, 
									Integer bits, Integer time_used){

		Integer bits_rhs = bits-(1<<(set_pos.elementAt(rhs)));
		time_ref.put(bits_rhs, time_used);
	}

	public static void Compute_FD(){
		long g_start = System.currentTimeMillis();
		for (int i = 0; i < lattice.size(); i++) {

			long bit_start = System.currentTimeMillis();
			Vector<String> set_str = GetSetChar(lattice.elementAt(i));
			Vector<Integer> set_pos = GetSetBit(lattice.elementAt(i));
			bit_related = bit_related + System.currentTimeMillis() - bit_start;
			Integer all_bit = -1;
			for (Integer j = 0; j < set_str.size(); j++) {
				long start = System.currentTimeMillis();
				if(check_subset(set_pos, j, lattice.elementAt(i))) 
					continue;
				check_subset_time = check_subset_time + (System.currentTimeMillis() - start);
				String query;
				Integer rtn = 0;

				//start = System.currentTimeMillis();

				Integer picked_val = (lattice.elementAt(i)-(1<<(set_pos.elementAt(j))));
				if (time_ref.containsKey(picked_val)) {
					rtn = time_ref.get(picked_val);
				}else{
					query = constrctQuery(set_str, set_str.elementAt(j));
					rtn = runQuery(query);
					add_time_ref(set_pos, j, lattice.elementAt(i), rtn);
				}

				if(all_bit == -1){

					if (time_ref.containsKey(lattice.elementAt(i))) {
						all_bit = time_ref.get(lattice.elementAt(i));
					}else{
						query = constrctQuery(set_str, "");
						all_bit = runQuery(query);
						time_ref.put(lattice.elementAt(i), all_bit);
					}
				}

				if (rtn.equals(all_bit) ) {
					print_depen(set_str, set_str.elementAt(j));

					start = System.currentTimeMillis();
					add_to_exist(set_pos, j, lattice.elementAt(i));
					add_to_exist_time = add_to_exist_time + (System.currentTimeMillis() - start);
				}
			}
		}
		cfd = cfd+ (System.currentTimeMillis() - g_start);
	}


	public static void main(String[] argv) {
  		long total = System.currentTimeMillis();

		init(argv);

		first_level();
		generate_next_level();
		Integer level = 1;
		//long start = System.currentTimeMillis();
		long computefd = 0;
		long generate = 0;
		while(lattice.size()!=0){
			//long start = System.currentTimeMillis();
			System.out.println("level is " + level);
			Compute_FD();
			//computefd += System.currentTimeMillis()-start;
			long start = System.currentTimeMillis();
			generate_next_level();
			generate += System.currentTimeMillis()-start;
			level++;
		}
		//long elapsedTimeMillis = System.currentTimeMillis()-start;
		System.out.println("total time: " + (System.currentTimeMillis() - total));
		System.out.println("generate: " + generate);
		System.out.println("query_time: " + query_time);
		System.out.println("check_subset_time: " + check_subset_time);

		// System.out.println("compute_fd: " + computefd);
		// System.out.println("check_subset_time: " + check_subset_time);
		// System.out.println("query_time: " + query_time);
		// System.out.println("statement_time: " + statement_time);
		// System.out.println("add_to_exist_time: " + add_to_exist_time);
		// System.out.println("cfd: " + cfd);
		// System.out.println("bit_related: " + bit_related);
		// System.out.println("query_times: " + query_times);
		// System.out.println("total time: " + (System.currentTimeMillis() - total));

	}
 
 	public static String constrctQuery(Vector<String> set_str, String s){
		String rtn = "select count(*) from (select distinct ";
    	String left = "";
    	for (Integer i = 0; i < set_str.size(); i++) {
        	if (set_str.elementAt(i) != s) {
            	if(left == "") left = set_str.elementAt(i);
            	else left = left + ',' + set_str.elementAt(i);
        	}
    	}
    	rtn = rtn + left + " from "+table + ") AS foo;";
		//System.out.println(rtn);
		return rtn;
	}

	public static Integer runQuery(String query){

		query_times++;
		try {
			long start = System.currentTimeMillis();

			st = connection.createStatement();
			statement_time = statement_time + (System.currentTimeMillis() - start);

			ResultSet rs = st.executeQuery(query);

			query_time = query_time + (System.currentTimeMillis() - start);

			Integer rtn = 0;
			if(rs.next()){
				rtn = Integer.parseInt(rs.getString(1));
			}else{
				System.out.println("ResultSet is empty--first level");
			}

			rs.close();
			st.close();
			return rtn;
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
 
		}
	}

	public static void generate_next_level(){
		HashSet<Integer> next_level = new HashSet<Integer>(0);
		for ( int i = 0; i < lattice.size(); i++ ) {
			for ( int j = i+1; j < lattice.size(); j++ ) {

				Integer combine_num = (( lattice.elementAt(i)) | (lattice.elementAt(j)));
				Integer com_size = countSetBits(combine_num) - 1;
				Integer l_size = countSetBits(lattice.elementAt(i));

				if (com_size.equals(l_size) ) {
					next_level.add( ( lattice.elementAt(i)) | 
					(lattice.elementAt(j)));
				}

			}
		}
		
		lattice = new Vector<Integer>(next_level);
	}

	public static Vector<String> GetSetChar(Integer n){
		Vector<String> rtn = new Vector<String>(0);

		for ( Integer i = 0; i < attr_num; i++ ) {
			if(((1<<i) & n)!=0) rtn.add(attrStr.elementAt(i));
		}
		return rtn;
	}

	public static Vector<Integer> GetSetBit(Integer n){
		Vector<Integer> rtn = new Vector<Integer>(0);

		for ( Integer i = 0; i < attr_num; i++ ) {
			if(((1<<i) & n)!=0) rtn.add(i);
		}
		return rtn;
	}

	public static Integer countSetBits(Integer n){
	    Integer count = 0;
	    while (n!=0){
	      n &= (n-1) ;
	      count++;
	    }
	    return count;
	}

	private static void init(String[] cmdinput){

 		attr_num = Integer.parseInt(cmdinput[0]);
 		table = cmdinput[1];
		attrStr = new Vector<String>(0);
		primaryKey = 0;
		lattice = new Vector<Integer>(0);
		depen_set = new Hashtable<Integer,HashSet<Integer>>(0);
		time_ref = new Hashtable<Integer, Integer>(0);
		check_subset_time = new Integer(0);
		add_to_exist_time = new Integer(0);
		query_time = new Integer(0);
		query_times = new Integer(0);
		attrStr.addElement("A");
		attrStr.addElement("B");
		attrStr.addElement("C");
		attrStr.addElement("D");
		attrStr.addElement("E");
		attrStr.addElement("F");
		attrStr.addElement("G");
		attrStr.addElement("H");
		attrStr.addElement("I");
		attrStr.addElement("J");
		attrStr.addElement("K");
		attrStr.addElement("L");
		attrStr.addElement("M");
		attrStr.addElement("N");
		attrStr.addElement("O");
		attrStr.addElement("P");
		attrStr.addElement("Q");
		attrStr.addElement("R");
		attrStr.addElement("S");
		attrStr.addElement("T");
		attrStr.addElement("U");
		attrStr.addElement("V");
		attrStr.addElement("W");
		attrStr.addElement("X");
		attrStr.addElement("Y");
		attrStr.addElement("Z");

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
 			connection.setAutoCommit(false);
 			st = connection.createStatement();

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
	}


}
