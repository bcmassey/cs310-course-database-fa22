package edu.jsu.mcis.cs310;

import java.sql.*;
import org.json.simple.*;
import org.json.simple.parser.*;

public class Database {
    
    private final Connection connection;
    
    private final int TERMID_SP22 = 1;
    
    //constructor
    public Database(String username, String password, String address) {
        
        this.connection = openConnection(username, password, address);
        
    }
    public String getSectionsAsJSON(int termid, String subjectid, String num) {
        
        String result = null;
        // INSERT YOUR CODE HERE
        try {
            //creating query
            String query = "SELECT * FROM section WHERE termid = ? AND subjectid = ? AND num = ?";
            PreparedStatement PrepStmnt = connection.prepareStatement(query);
            PrepStmnt.setInt(1, termid);
            PrepStmnt.setString(2, subjectid);
            PrepStmnt.setString(3, num);
            ResultSet rslts = PrepStmnt.executeQuery();
            //Convert to Json
            result = getResultSetAsJSON(rslts); 

            //rslts.close();
            
        } catch(Exception e){e.printStackTrace();}
       
        
        return result;
        
    }
    
    public int register(int studentid, int termid, int crn) {
        
        int result = 0;
        // INSERT YOUR CODE HERE "This will involve inserting a new record into the 'registration'"
        try{
            String query = "INSERT INTO registration (studentid, termid, crn) VALUES (?,?,?)";
            PreparedStatement PrepStmnt = connection.prepareStatement(query);
            PrepStmnt.setInt(1, studentid);
            PrepStmnt.setInt(2, termid);
            PrepStmnt.setInt(3, crn);
            //updating
            result = PrepStmnt.executeUpdate(); 
            
        } catch(Exception e){e.printStackTrace();}
       
        return result;
        
    }

    public int drop(int studentid, int termid, int crn) {
        
        int result = 0;        
        // INSERT YOUR CODE HERE "This will involve a query which deletes the corresponding record from the 'registration' table"
        try{
            String query = "DELETE FROM registration WHERE studentid = ? AND termid = ? AND crn = ?";
            PreparedStatement PrepStmnt = connection.prepareStatement(query);
            PrepStmnt.setInt(1, studentid);
            PrepStmnt.setInt(2, termid);
            PrepStmnt.setInt(3, crn);
            //updating
            result = PrepStmnt.executeUpdate(); 
            
        }catch(Exception e){e.printStackTrace();}
        
        return result;
        
    }
    
    public int withdraw(int studentid, int termid) {
       
        int result = 0;       
        // INSERT YOUR CODE HERE "This will again involve deleting records from the 'registration' table"
        try{
            String query = "DELETE FROM registration WHERE studentid = ? AND termid = ?";
            PreparedStatement PrepStmnt = connection.prepareStatement(query);
            PrepStmnt.setInt(1, studentid);
            PrepStmnt.setInt(2, termid);
            //updating
            result = PrepStmnt.executeUpdate(); 
            
        }catch(Exception e){e.printStackTrace();}
        
        return result;
        
    }
    
    public String getScheduleAsJSON(int studentid, int termid) {
        
        String result = null;
        JSONArray json = new JSONArray();
        int rows = 0;
        int[] crn; 
        // INSERT YOUR CODE HERE "The query results should return all information about the registered sections, as given in the 'section' table
        try {
            String query = "SELECT * FROM registration WHERE studentid = ? AND termid = ?";
            PreparedStatement PrepStmnt = connection.prepareStatement(query);
            PrepStmnt.setInt(1, studentid);
            PrepStmnt.setInt(2, termid);
            ResultSet rslts = PrepStmnt.executeQuery();
            
            while(rslts.next()) {
                rows++;
            }
            crn = new int[rows];
            rslts = PrepStmnt.executeQuery();

            for(int i = 0; i < rows; i++) {
                rslts.next();
                crn[i] = rslts.getInt(3);
            }
            for(int i = 0; i < crn.length; i++) {  
                query = "SELECT scheduletypeid, instructor, num, `start`, days, section, `end`, `where`, crn, subjectid FROM section WHERE crn = ?";
                PrepStmnt = connection.prepareStatement(query);
                PrepStmnt.setInt(1, crn[i]);
                
                rslts = PrepStmnt.executeQuery();
                ResultSetMetaData metadata = rslts.getMetaData();
                int columnCount = metadata.getColumnCount();

                while (rslts.next()) {
                    JSONObject obj = new JSONObject();
                    obj.put("studentid", Integer.toString(studentid));
                    obj.put("termid", Integer.toString(termid));

                    for (int a = 1; a <= columnCount; a++) { 
                        String key = metadata.getColumnName(a);
                        String value = rslts.getString(a);
                        obj.put(key, value);
                    }
                    json.add(obj);
                }
            }
            //Converts JSONArray to JSON String
            result = JSONValue.toJSONString(json); 
        }catch (Exception e) {e.printStackTrace();}
        
        return result;
        
    }
    
    public int getStudentId(String username) {
        
        int id = 0;
        
        try {
        
            String query = "SELECT * FROM student WHERE username = ?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, username);
            
            boolean hasresults = pstmt.execute();
            
            if ( hasresults ) {
                
                ResultSet resultset = pstmt.getResultSet();
                
                if (resultset.next())
                    
                    id = resultset.getInt("id");
                
            }
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return id;
        
    }
    
    public boolean isConnected() {

        boolean result = false;
        
        try {
            
            if ( !(connection == null) )
                
                result = !(connection.isClosed());
            
        }
        catch (Exception e) { e.printStackTrace(); }
        
        return result;
        
    }
    
    /* PRIVATE METHODS */

    private Connection openConnection(String u, String p, String a) {
        
        Connection c = null;
        
        if (a.equals("") || u.equals("") || p.equals(""))
            
            System.err.println("*** ERROR: MUST SPECIFY ADDRESS/USERNAME/PASSWORD BEFORE OPENING DATABASE CONNECTION ***");
        
        else {
        
            try {

                String url = "jdbc:mysql://" + a + "/jsu_sp22_v1?autoReconnect=true&useSSL=false&zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=America/Chicago";
                // System.err.println("Connecting to " + url + " ...");

                c = DriverManager.getConnection(url, u, p);

            }
            catch (Exception e) { e.printStackTrace(); }
        
        }
        
        return c;
        
    }
    
    private String getResultSetAsJSON(ResultSet resultset) {
        
        String result;
        
        /* Create JSON Containers */
        
        JSONArray json = new JSONArray();
        JSONArray keys = new JSONArray();
        
        try {
            
            /* Get Metadata */
        
            ResultSetMetaData metadata = resultset.getMetaData();
            int columnCount = metadata.getColumnCount();
            
            // INSERT YOUR CODE HERE
            while (resultset.next()){
                JSONObject obj = new JSONObject();
                for (int i = 1; i <= columnCount; i++){
                    String key = metadata.getColumnName(i);
                    String value = resultset.getString(i);
                    keys.add(key);
                    obj.put(key, value);
                }
                json.add(obj);
            }
        }catch (Exception e) { e.printStackTrace(); }
        
        /* Encode JSON Data and Return */
        
        result = JSONValue.toJSONString(json);
        return result;
        
    }
    
}