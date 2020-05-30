package Ex;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Formatter;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Batch {

    public static void main(String[] args) {
    	
    	Connection con = null;
        PreparedStatement pst = null;
        ResultSet rs = null;
        Statement st = null;
        FileInputStream fin = null;
        FileInputStream fin2 = null;
        FileInputStream fin3 = null;




        Properties props = new Properties();
        FileInputStream in = null;

        try {
            in = new FileInputStream("BancoDeDados_db.properties");
            props.load(in);

        } catch (FileNotFoundException ex) {

            Logger lgr = Logger.getLogger(Batch.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } catch (IOException ex) {

            Logger lgr = Logger.getLogger(Batch.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } finally {
            
            try {
                 if (in != null) {
                     in.close();
                 }
            } catch (IOException ex) {
                Logger lgr = Logger.getLogger(Batch.class.getName());
                lgr.log(Level.SEVERE, ex.getMessage(), ex);
            }
        }

        String url = props.getProperty("db.url");
        String user = props.getProperty("db.user");
        String passwd = props.getProperty("db.passwd");

        try {	
        	
            con = DriverManager.getConnection(url, user, passwd);
            
//            con.setAutoCommit(false);
//            st = con.createStatement();
	            
            
            pst = con.prepareStatement("INSERT INTO Authors(Name) VALUES('Augusto Cury');", Statement.RETURN_GENERATED_KEYS);
            pst.executeUpdate();
            ResultSet key = pst.getGeneratedKeys();
            
            
            if (key.first()) {
                System.out.printf("O ID do novo autor é: %d", key.getLong(1));
                
                pst = con.prepareStatement("INSERT INTO Books(Id, AuthorId, Title) VALUES(53, ?,'O vendedor de sonhos');");
                pst.setLong(1, key.getLong(1));
                pst.executeUpdate();
                
                pst = con.prepareStatement("INSERT INTO Books(Id, AuthorId, Title) VALUES(54, ?,'O homem mais inteligente da historia');");
                pst.setLong(1, key.getLong(1));
                pst.executeUpdate();
                
                pst = con.prepareStatement("INSERT INTO Books(Id, AuthorId, Title) VALUES(55, ?,'Nunca desista de seus sonhos');");
                pst.setLong(1, key.getLong(1));
                pst.executeUpdate();
                 	
            }

            
            File img = new File("1.png");
            fin = new FileInputStream(img);
            
            pst = con.prepareStatement("INSERT INTO Images(Data) VALUES(?)");
            pst.setBinaryStream(1, fin, (int) img.length());
            pst.executeUpdate();
            
            
            File img2 = new File("2.png");
            fin2 = new FileInputStream(img2);
            
            pst = con.prepareStatement("INSERT INTO Images(Data) VALUES(?)");
            pst.setBinaryStream(1, fin, (int) img.length());
            pst.executeUpdate();
            
            File img3 = new File("3.png");
            fin3 = new FileInputStream(img3);
            
            pst = con.prepareStatement("INSERT INTO Images(Data) VALUES(?)");
            pst.setBinaryStream(1, fin, (int) img.length());
            pst.executeUpdate();
            
           String query = "SELECT Name, Title From Authors, "
                   + "Books WHERE Authors.Id=Books.AuthorId";
           pst = con.prepareStatement(query);

           rs = pst.executeQuery();

           ResultSetMetaData meta = rs.getMetaData();

           String colname1 = meta.getColumnName(1);
           String colname2 = meta.getColumnName(2);

  
           Formatter fmt1 = new Formatter();
           fmt1.format("%-21s%s", colname1, colname2);
           System.out.println(fmt1);

           while (rs.next()) {

               Formatter fmt2 = new Formatter();
               fmt2.format("%-21s", rs.getString(1));
               System.out.print(fmt2);
               System.out.println(rs.getString(2));
           }

        	
        } catch (Exception ex) {
            
            Logger lgr = Logger.getLogger(Batch.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);

        } finally {

            try {
                
                if (rs != null) {
                    rs.close();
                }
                
                if (pst != null) {
                    pst.close();
                }
                
                if (con != null) {
                    con.close();
                }

            } catch (SQLException ex) {
                
                Logger lgr = Logger.getLogger(Batch.class.getName());
                lgr.log(Level.WARNING, ex.getMessage(), ex);
            }
        }
    }

}
