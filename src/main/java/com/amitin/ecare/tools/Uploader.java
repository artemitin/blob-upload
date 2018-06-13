package de.tmobile.ecare.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Created by amitin on 01.09.2015.
 */
public class Uploader {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        System.out.println("Usage: java -jar blob-upload.jar <path to .properties (optional)>");
        String propsPath = "config.properties";
        if (args.length > 0) {
            propsPath = args[0];
            System.out.print("Using user-specified properties path: ");
        } else {
            System.out.print("Using default properties path: ");
        }
        System.out.println(propsPath);

        Properties props = new Properties();
        try {
            props.load(new FileInputStream(propsPath));
        } catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
            System.out.println("To use default properties put 'config.properties' at the same level to jar");
            System.out.println("Specify the following properties: \n" +
                            "connection.host\n" +
                            "connection.port\n" +
                            "connection.sid\n" +
                            "connection.username\n" +
                            "connection.password\n" +
                            "pdfs.path\n"
            );
            System.exit(0);
        }

        Class.forName("oracle.jdbc.OracleDriver");

        Connection con = DriverManager.getConnection("jdbc:oracle:thin:" +
                props.getProperty("connection.username") + "/" + props.getProperty("connection.password") + "@" +
                props.getProperty("connection.host") + ":" + props.getProperty("connection.port") + ":" +
                props.getProperty("connection.sid"));

        File pdfsDir = new File(props.getProperty("pdfs.path"));
        File[] pdfs = pdfsDir.listFiles();

        if (pdfs != null) {
            try {
                System.out.println("Checking DB for existing pdfs...");
                Statement selectStmt = con.createStatement();
                ResultSet rs = selectStmt.executeQuery("select PDF_NAME from KSA$TA_PDF_TEMPLATE");
                Set<String> existingInDB = new HashSet<String>();
                while (rs.next()) {
                    String existingPdfName = rs.getString("PDF_NAME");
                    existingInDB.add(existingPdfName);
                    System.out.println("Found existing pdf: " + existingPdfName);
                }
                rs.close();

                PreparedStatement insertStmt = con.prepareStatement("insert into KSA$TA_PDF_TEMPLATE(PDF_NAME) values (?)");
                for (File file : pdfs) {
                    String fileNameExt = file.getName();
                    String fileName = fileNameExt.substring(0, fileNameExt.indexOf(".pdf"));
                    if (!existingInDB.contains(fileName)) {
                        System.out.println("New row will be inserted for: " + fileName);
                        insertStmt.setString(1, fileName);
                        insertStmt.addBatch();
                    }
                }

                insertStmt.executeBatch();
                con.commit();
                System.out.println("Commit successfull");

                PreparedStatement updateStmt = con.prepareStatement("update KSA$TA_PDF_TEMPLATE set PDF_DOCUMENT = ? where PDF_NAME = ?");

                for (File file : pdfs) {
                    String fileNameExt = file.getName();
                    String fileName = fileNameExt.substring(0, fileNameExt.indexOf(".pdf"));
                    System.out.println("Preparing to upload: " + fileName);
                    InputStream is = new FileInputStream(file);
                    updateStmt.setBinaryStream(1, is);
                    updateStmt.setString(2, fileName);
                    updateStmt.addBatch();
                }
                updateStmt.executeBatch();
                con.commit();
                System.out.println("Commit successfull");

            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                con.close();
            }
        } else {
            System.out.println(pdfsDir.getPath() + " is not a directory or unknown error occured.");
        }
    }
}