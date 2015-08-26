package algorithm;
import container.DifferenceSet;
//import container.FunctionalDependencyGroup2;
//import generator.CouldNotReceiveResultException;
//import generator.FunctionalDependency;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.ListIterator;

import org.apache.lucene.util.OpenBitSet;

public class dbpuller {
	private static Connection connection;
	private static Integer numberOfAttributes;
	private static String table;
	private static long query_time;
	private static Vector<String> attrStr;
	private static Statement st;

	
	private static void init(String[] cmdinput){

		numberOfAttributes = Integer.parseInt(cmdinput[0]);
 		table = cmdinput[1];
		attrStr = new Vector<String>(0);
//		primaryKey = 0;
//		lattice = new Vector<Integer>(0);
//		depen_set = new Hashtable<Integer,HashSet<Integer>>(0);
//		time_ref = new Hashtable<Integer, Integer>(0);
//		check_subset_time = new Integer(0);
//		add_to_exist_time = new Integer(0);
		query_time = new Integer(0);
//		query_times = new Integer(0);
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
					"jdbc:postgresql://127.0.0.1:5432/zhangshuopeng", "zhangshuopeng",
					"");
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
			
			String add_id = "alter table"
					+ table
					+ "add id SERIAL";
			 
			String inner_join = "CREATE VIEW joined AS"
					+ "SELECT t1.a AS A1, t2.a AS A2, t1.b AS B1, t2.b AS B2, t1.c AS C1, t2.c AS C2, t1.d AS D1, t2.d AS D2,"
					+ "t1.e AS E1, t2.e AS E2, t1.f AS F1, t2.f AS F2"
					+ "FROM fastfd t1"
					+ "INNER JOIN fastfd t2"
					+ "ON t1.id <t2.id";
			
			String diff_set = "CREATE VIEW diffset AS"
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
					
			String diffset_output = "CREATE VIEW diffset_output AS"
					+ "SELECT id, array_agg(diff) AS DifferenceSets"
					+ "FROM diffset"
					+ "GROUP BY id";
			
			String receive_diffset = "SELECT * FROM diffset_output";
			
			long start1 = System.currentTimeMillis();
			Statement st1 = connection.createStatement();
			st1.executeUpdate(add_id);
			System.out.println("ID column added");
			query_time = query_time + (System.currentTimeMillis() - start1);
			
			long start2 = System.currentTimeMillis();
			Statement st2 = connection.createStatement();
			st2.executeUpdate(inner_join);
			System.out.println("Joined view created");
			query_time = query_time + (System.currentTimeMillis() - start2);
			
			long start3 = System.currentTimeMillis();
			Statement st3 = connection.createStatement();
			st3.executeUpdate(diff_set);
			System.out.println("Differences spotted");
			query_time = query_time + (System.currentTimeMillis() - start3);
			
			long start4 = System.currentTimeMillis();
			Statement st4 = connection.createStatement();
			st4.executeUpdate(diffset_output);
			System.out.println("Diffset view created");
			query_time = query_time + (System.currentTimeMillis() - start4);
			
			long start5 = System.currentTimeMillis();
			Statement st5 = connection.createStatement();
			ResultSet rs = st5.executeQuery(receive_diffset);
			query_time = query_time + (System.currentTimeMillis() - start5);
			
			if(rs.next()){
			
				Integer first = rs.getInt(1);
			    String second = rs.getString(2);
			    
			    
			}
		} catch(SQLException e) {
			System.out.println("Connection Failed! Check output console");
			return;
		}
	}

	
	
	
	
	
	
	
	
	
	
	
    
	private void findcover(List<DifferenceSet> differenceSets, int numberOfAttributes){
		for (int attribute = 0; attribute < numberOfAttributes; attribute++) {

	        List<DifferenceSet> tempDiffSet = new LinkedList<DifferenceSet>();

	        // Compute DifferenceSet modulo attribute (line 3 - Fig5 - FastFDs)
	        for (DifferenceSet ds : differenceSets) {
	            OpenBitSet obs = ds.getAttributes().clone();
	            if (!obs.get(attribute)) {
	                continue;
	            } else {
	                obs.flip(attribute);
	                tempDiffSet.add(new DifferenceSet(obs));
	            }
	        }

	        // check new DifferenceSet (line 4 + 5 - Fig5 - FastFDs)
	        if (tempDiffSet.size() == 0) {
	            System.out.println("No diffsets containing" + attribute);
	        } else if (this.checkNewSet(tempDiffSet)) {
	            List<DifferenceSet> copy = new LinkedList<DifferenceSet>();
	            copy.addAll(tempDiffSet);
	            this.doRecusiveCrap(attribute, this.generateInitialOrdering(tempDiffSet), copy, new IntArrayList(), tempDiffSet);
	        }

	    }
	}

		private boolean checkNewSet(List<DifferenceSet> tempDiffSet) {

	        for (DifferenceSet ds : tempDiffSet) {
	            if (ds.getAttributes().isEmpty()) {
	                return false;
	            }
	        }

	        return true;
	    }

	    private IntList generateInitialOrdering(List<DifferenceSet> tempDiffSet) {

	        IntList result = new IntArrayList();

	        Int2IntMap counting = new Int2IntArrayMap();
	        for (DifferenceSet ds : tempDiffSet) {

	            int lastIndex = ds.getAttributes().nextSetBit(0);

	            while (lastIndex != -1) {
	                if (!counting.containsKey(lastIndex)) {
	                    counting.put(lastIndex, 1);
	                } else {
	                    counting.put(lastIndex, counting.get(lastIndex) + 1);
	                }
	                lastIndex = ds.getAttributes().nextSetBit(lastIndex + 1);
	            }
	        }

	        // TODO: Comperator und TreeMap --> Tommy
	        while (true) {

	            if (counting.size() == 0) {
	                break;
	            }

	            int biggestAttribute = -1;
	            int numberOfOcc = 0;
	            for (int attr : counting.keySet()) {

	                if (biggestAttribute < 0) {
	                    biggestAttribute = attr;
	                    numberOfOcc = counting.get(attr);
	                    continue;
	                }

	                int tempOcc = counting.get(attr);
	                if (tempOcc > numberOfOcc) {
	                    numberOfOcc = tempOcc;
	                    biggestAttribute = attr;
	                } else if (tempOcc == numberOfOcc) {
	                    if (biggestAttribute > attr) {
	                        biggestAttribute = attr;
	                    }
	                }
	            }

	            if (numberOfOcc == 0) {
	                break;
	            }

	            result.add(biggestAttribute);
	            counting.remove(biggestAttribute);
	        }

	        return result;
	    }

	    private void doRecusiveCrap(int currentAttribute, IntList currentOrdering, List<DifferenceSet> setsNotCovered,
	                                IntList currentPath, List<DifferenceSet> originalDiffSet)
	    {

	        // Basic Case
	        // FIXME
	        if (!currentOrdering.isEmpty() && /* BUT */setsNotCovered.isEmpty()) {
	                System.out.println("no FDs here");
	            return;
	        }

	        if (setsNotCovered.isEmpty()) {

	            List<OpenBitSet> subSets = this.generateSubSets(currentPath);
	            if (this.noOneCovers(subSets, originalDiffSet)) {
	                System.out.println(currentAttribute + "->" + "currentPath");
	            } else {
	
	                    System.out.println("FD not minimal");
	                    System.out.println(currentAttribute + "->" + currentPath);
	            
	            }

	            return;
	        }

	        // Recusive Case
	        for (int i = 0; i < currentOrdering.size(); i++) {

	            List<DifferenceSet> next = this.generateNextNotCovered(currentOrdering.getInt(i), setsNotCovered);
	            IntList nextOrdering = this.generateNextOrdering(next, currentOrdering, currentOrdering.getInt(i));
	            IntList currentPathCopy = new IntArrayList(currentPath);
	            currentPathCopy.add(currentOrdering.getInt(i));
	            this.doRecusiveCrap(currentAttribute, nextOrdering, next, currentPathCopy, originalDiffSet);
	        }

	    }

	    private IntList generateNextOrdering(List<DifferenceSet> next, IntList currentOrdering, int attribute) {

	        IntList result = new IntArrayList();

	        Int2IntMap counting = new Int2IntArrayMap();
	        boolean seen = false;
	        for (int i = 0; i < currentOrdering.size(); i++) {

	            if (!seen) {
	                if (currentOrdering.getInt(i) != attribute) {
	                    continue;
	                } else {
	                    seen = true;
	                }
	            } else {

	                counting.put(currentOrdering.getInt(i), 0);
	                for (DifferenceSet ds : next) {

	                    if (ds.getAttributes().get(currentOrdering.getInt(i))) {
	                        counting.put(currentOrdering.getInt(i), counting.get(currentOrdering.getInt(i)) + 1);
	                    }
	                }
	            }
	        }

	        // TODO: Comperator und TreeMap --> Tommy
	        while (true) {

	            if (counting.size() == 0) {
	                break;
	            }

	            int biggestAttribute = -1;
	            int numberOfOcc = 0;
	            for (int attr : counting.keySet()) {

	                if (biggestAttribute < 0) {
	                    biggestAttribute = attr;
	                    numberOfOcc = counting.get(attr);
	                    continue;
	                }

	                int tempOcc = counting.get(attr);
	                if (tempOcc > numberOfOcc) {
	                    numberOfOcc = tempOcc;
	                    biggestAttribute = attr;
	                } else if (tempOcc == numberOfOcc) {
	                    if (biggestAttribute > attr) {
	                        biggestAttribute = attr;
	                    }
	                }
	            }

	            if (numberOfOcc == 0) {
	                break;
	            }

	            result.add(biggestAttribute);
	            counting.remove(biggestAttribute);
	        }

	        return result;
	    }

	    private List<DifferenceSet> generateNextNotCovered(int attribute, List<DifferenceSet> setsNotCovered) {

	        List<DifferenceSet> result = new LinkedList<DifferenceSet>();

	        for (DifferenceSet ds : setsNotCovered) {

	            if (!ds.getAttributes().get(attribute)) {
	                result.add(ds);
	            }
	        }

	        return result;
	    }

//	    private void addFdToReceivers(FunctionalDependencyGroup2 fdg){

//	        FunctionalDependency fd = fdg.buildDependency(this.tableIdentifier, this.columnNames);
//	        this.receiver.receiveResult(fd);

//	    }

	    private boolean noOneCovers(List<OpenBitSet> subSets, List<DifferenceSet> originalDiffSet) {

	        for (OpenBitSet obs : subSets) {

	            if (this.covers(obs, originalDiffSet)) {
	                return false;
	            }

	        }

	        return true;
	    }

	    private boolean covers(OpenBitSet obs, List<DifferenceSet> originalDiffSet) {

	        for (DifferenceSet diff : originalDiffSet) {

	            if (OpenBitSet.intersectionCount(obs, diff.getAttributes()) == 0) {
	                return false;
	            }
	        }

	        return true;
	    }

	    private List<OpenBitSet> generateSubSets(IntList currentPath) {

	        List<OpenBitSet> result = new LinkedList<OpenBitSet>();

	        OpenBitSet obs = new OpenBitSet();
	        for (int i : currentPath) {
	            obs.set(i);
	        }

	        for (int i : currentPath) {

	            OpenBitSet obs_ = obs.clone();
	            obs_.flip(i);
	            result.add(obs_);

	        }

	        return result;
	    }
		
		
	}


	
	
	
	
	
	
	
	
	

