package jonathan.mayer.calciteh2;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws SQLException
    {
       
    	String connstr = "jdbc:h2:file:D:\\H2Data\\testing";
    	Connection conn = DriverManager.getConnection(connstr,"sa","sa");
    	
    	 Statement st = conn.createStatement();
    	 ResultSet result = st.executeQuery("SELECT * from VW_STFUNCTIONS");
    	 while ( result.next() ) { 
    		 
    		 String stname = result.getString("STNAME");
    		 String returntype = result.getString("RETURNTYPE");
    		 getfunctionvariants(stname);
    		 System.out.println(returntype + " " + stname + "()");
    	 }
    	 conn.close();
    	
//    	System.out.println( "Hello World!" );
    }
    
    public static int getfunctionvariants(String stname) throws SQLException {
    	int total = 0;
    	String connstr = "jdbc:h2:file:D:\\H2Data\\testing";
    	Connection conn = DriverManager.getConnection(connstr,"sa","sa");
    	
    	 Statement st = conn.createStatement();
    	 
    	 ResultSet result = st.executeQuery("\"SELECT STNAME,VARIANTNAME,count(*) total from VW_STVARIANTS WHERE STNAME='" + stname + "' GROUP BY STNAME,VARIANTNAME;");
    	 while ( result.next() ) { 
    		 
    		 total = result.getInt("total");
    		
    		 System.out.println(total);
    	 }
    	 conn.close();
    	
    	return total;
    	
    	
    }
}
