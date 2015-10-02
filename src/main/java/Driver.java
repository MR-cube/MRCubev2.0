
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class Driver {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
   
   public static void main(String[] args) throws SQLException, ClassNotFoundException {
      // Register driver and create driver instance
	   System.out.println("Hive JDBC init...");
	   
	   try {
		   Class.forName(driverName);
		   Connection connection = null;
		   System.out.println("Before getting connection :");
		   connection= DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "root", "hadoop");
		   System.out.println("After getting connection ");
		   ResultSet resultSet = connection.createStatement().executeQuery("select count(*) as cnt from bidid_1");;
		   while (resultSet.next()) {
			   System.out.println(resultSet.getString(1));
		   }
	   } catch (Exception e) {
		   e.printStackTrace();
		   System.exit(1);
	   }
   }
}
