package com.database.migration;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.apache.commons.io.FileUtils;

import java.sql.Types;


/**
 * @author adamszczesiak 23 mar 2016
 */
public class Migration
{
    /**
     * 
     * @param args:  mssql/mssql-integratedSecurity server:port templateDBName targetDBName user password unusedTables 
     * postgresql server:port templateDBName targetDBName user password unusedTables pg_dumpPath psqlPath
     * @arg[0]: export-postgresql/export-mssql/import-postgresql/import-mssql 
     * 
     * postgresql localhost:5432 dvdrental skopiowana pguser pguser "C:\Program Files\PostgreSQL\9.5\bin\pg_dump" "C:\Program Files\PostgreSQL\9.5\bin\psql"
     * 
     * export-postgresql targetZipDirectory pg_dumpPath host port userName password databaseName
     * export-postgresql C:\\Users\\Adam\\Desktop\\ "C:\\Program Files\\PostgreSQL\\9.5\\bin\\pg_dump" localhost 5432 pguser pguser dvdrental adminDBName
     * import-postgresql  "C:\\Program Files\\PostgreSQL\\9.5\\bin\\psql.exe"  localhost 5432 pguser pguser dvdrental C://Users//Adam//Desktop//dvdrental.zip postgres(adminDBName)
     * @throws SQLException
     * @throws ClassNotFoundException
     * pg_dump path : C:\\Program Files\\PostgreSQL\\9.5\\bin\\pg_dump
     * @throws IOException 
     * 
     * import-mssql trunk testowa adam password localhost:1433 C://Users//Adam//Desktop//file.zip
     * export-mssql integratedSecurity databaseName hostAndPort zipPath
     * export-mssql databaseName userName password hostAndPort zipPath
     */
    public static void main( String[] args ) throws SQLException, ClassNotFoundException, IOException
    {
        boolean result = false;
        int argsLength = args.length;
        boolean integratedSecurity = false;
        List<String> tablesNames = new ArrayList<String>();
        
        if(args[0].equals("export-postgresql")) {
        	if(args.length < 8) {
        		System.out.println("Not enough arguments provided.");
        	} else {
        		String targetZipDirectoryPath = args[1];
        		String pg_dumpPath = args[2];
        		String host = args[3];
        		String port = args[4];
        		String userName = args[5];
        		String password = args[6];
        		String databaseName = args[7];
        		System.out.println("Exporting database to zip file...");
                exportPostgresToSQLFile(pg_dumpPath, host, 
                		port, userName, password, databaseName, System.getProperty("user.home") + File.separator + databaseName + ".backup");
                addDatabaseToZipArchive(databaseName, targetZipDirectoryPath);
        	}
            
        } else if(args[0].equals("export-mssql")) {
        	System.out.println("Exporting mssql database to zip file");
        	String dbAddress = "";
        	String userName = "";
        	String password = "";
        	String databaseType = "";
        	String hostAndPort = "";
        	String databaseName = "";
        	String zipPath = "";
        	Connection connection = null;
        	
        	if(args[1].equals("integratedSecurity")) {
        		integratedSecurity = true;
        	}
        	
        	/**
        	 * export-mssql integratedSecurity databaseName hostAndPort zipPath
        	 */
        	if(integratedSecurity) {
        		if(args.length < 5) {
        			System.out.println("Not enough arguments provided.");
        			return;
        		}
        		databaseType = "mssql-integratedSecurity";
        		databaseName = args[2];
        		hostAndPort = args[3];
        		zipPath = args[4];
        		dbAddress = "//" + args[2];
        		connection = getDatabaseConnection(dbAddress, userName, password, databaseType, hostAndPort, databaseName, integratedSecurity);
        	}
        	/**
        	 * export-mssql databaseName userName password hostAndPort zipPath
        	 */
        	else {
        		if(args.length < 6) {
        			System.out.println("Not enough arguments provided.");
        			return;
        		}
        		databaseType = "mssql";
        		dbAddress = "//" + args[1];
        		databaseName = args[1];
        		userName = args[2];
        		password = args[3];
        		hostAndPort = args[4];
        		zipPath = args[5];
        		connection = getDatabaseConnection(dbAddress, userName, password, databaseType, hostAndPort, databaseName, integratedSecurity);
        	}
        	createMSSQLBackup(databaseName, connection);
        	addMssqlDatabaseToZipArchive(databaseName, zipPath);
        	System.out.println("Zip file created in selected location");
        	
        } else if(args[0].equals("import-postgresql")) {
        	if(args.length  < 9) {
        		System.out.println("Not enough arguments provided.");
        	} else {
        		String psqlPath = args[1];
        		String host = args[2];
        		String port = args[3];
        		String userName = args[4];
        		String password = args[5];
        		String databaseName = args[6];
        		String fullZipPath = args[7];
        		String adminDatabaseName = args[8];
            	System.out.println("Importing postgreSQL database from zip file...");
            	restorePostgresqlDatabase(psqlPath, host, port, userName, password, databaseName, fullZipPath, adminDatabaseName);	
        	}
        	/**
        	 * import-mssql databaseName adminDatabase userName password hostAndPort zipPath
        	 */
        } else if(args[0].equals("import-mssql")) {
        	
        	String databaseName = args[1];
        	String adminDatabase = args[2];
        	String userName = args[3];
        	String password = args[4];
        	String hostAndPort = args[5];
        	String zipPath = args[6];
        	System.out.println("Importing database from zip file...");
        	restoreMssqlDatabaseFromZip(databaseName, adminDatabase, userName, password, hostAndPort, zipPath);
        } else {
 
            System.out.println("Performing database migration...");
            
            if(!args[0].equals("mssql-integratedSecurity")) {
            	if(argsLength < 7) {
                    System.out.println("Not all arguments provided!");
                    return;
                }	
            } else {
            	integratedSecurity = true;
            	if(argsLength < 4) {
            		System.out.println("Not all arguments provided!");
                    return;
            	}
            }
            
            String adminDatabaseAddress = "";
            String adminDatabaseName = "";
            String databaseType = args[0];
            String dbAdress = "//" + args[1];     //"localhost:5432";
            String templateDatabaseName = args[2];
            String targetDatabaseName = args[3];
            String userName = "";
            String password = "";
            String pg_dumpPath = "";
            String psqlPath = "";
            if(databaseType.equals("postgresql")) {
            	pg_dumpPath = args[6];
            	psqlPath = args[7];
            }
            if(!integratedSecurity) {
            	adminDatabaseAddress = "//" + args[1] + "/" + args[6];
            	adminDatabaseName = args[6];
                userName = args[4];    //"postgres";
                password = args[5];    //"postgres";
            }

            String dbAdressCopy = dbAdress + "/" + args[3];
            dbAdress += "/" + args[2];
            
            String hostAndPort = args[1];
         
            
            tablesNames = getArgsTablesNames(args, integratedSecurity, databaseType);          
                   
            Connection connection = getDatabaseConnection(dbAdress, userName, password, databaseType, hostAndPort, templateDatabaseName, integratedSecurity);
            
            for(int i = 0; i < tablesNames.size(); i++) {
            	System.out.println(tablesNames.get(i));
            }
            List<String> allTablesNames = getDatabaseTablesNames(connection);
            result = copySchema(connection, templateDatabaseName, targetDatabaseName, userName, password, 
            		hostAndPort, allTablesNames,tablesNames, integratedSecurity,
            		pg_dumpPath, psqlPath);
                        
            if(result) {
                System.out.println("Database migrated successfully.");
            } else {
                System.out.println("Database migration terminated.");
            }
        }                 
    }
    
    public static String getHostFromAddress(String address) {
    	String[] data = address.split(":");
    	return data[0];
    }
    
    public static String getPortFromAddress(String address) {
    	String[] data = address.split(":");
    	return data[1];
    }
    
    public static List<String> getArgsTablesNames(String[] args, boolean integratedSecurity, String databaseType) {
    	List<String> tablesNames = new ArrayList<String>();
    	if(integratedSecurity) {
    		for(int i = 4; i < args.length; i++) {
                tablesNames.add(args[i]);
            }
    	} else if(databaseType.equals("postgresql")) {
    		for(int i = 8; i < args.length; i++) {
                tablesNames.add(args[i]);
            }
    	} else if(databaseType.equals("mssql")) {
    		for(int i = 7; i < args.length; i++) {
                tablesNames.add(args[i]);
            }
    	}
    	return tablesNames;
    }
    
    public static void createDatabaseWithTemplate(Connection connection, String databaseName, 
    		String templateSchema, String templateDatabase) {
    	String query = "CREATE DATABASE " + databaseName + " WITH TEMPLATE " + templateDatabase + ";";
    	Statement statement;
    	try
        {
            statement = connection.createStatement();
            statement.executeUpdate(query);
            connection.close();
        }
        catch ( SQLException e )
        {
            e.printStackTrace();
        }
    }
    
    public static List<String> checkDatabaseTablesNames(List<String> databaseTablesNames, List<String> unusedTablesNames) {
        List<String> wrongTablesNames = new ArrayList<String>();
        
        for(int i = 0; i < unusedTablesNames.size(); i++) {
            if(!databaseTablesNames.contains( unusedTablesNames.get(i))) {
                wrongTablesNames.add( unusedTablesNames.get(i) );
            }
        }
        return wrongTablesNames;
    }
    
    
    public static boolean copySchema(Connection connection, String templateDatabaseName, String targetDatabaseName,
    		String userName, String password, String hostAndPort, List<String> allTablesNames,
    		List<String> unusedTablesNames, boolean integratedSecurity, String pg_dumpPath, String psqlPath) throws SQLException {
    	
        boolean result = false;
        DatabaseMetaData metaData = connection.getMetaData();
        String databaseType = metaData.getDatabaseProductName();
        if(databaseType.equals("Microsoft SQL Server")) {
        	String dbAddress = "//" + hostAndPort;
        	List<String> tablesToCopy = removeElementsFromList(allTablesNames, unusedTablesNames);
        	createDatabaseMigrationDirectory();
        	createDatabaseMsSQL(targetDatabaseName, templateDatabaseName,dbAddress,userName, password, integratedSecurity, hostAndPort);
        	
        	Connection connectionCopy = getConnectionMsSQL(dbAddress, userName, password, hostAndPort, targetDatabaseName, integratedSecurity);
        	copyMsSQLTablesSchemaToTargetDatabase(connection, connectionCopy);
        	result = copyMsSQLTablesContent(connectionCopy, templateDatabaseName, tablesToCopy);
        	
        } else if(databaseType.equals("PostgreSQL")) {
        	String dbAddress = "//" + hostAndPort + "/" + targetDatabaseName;
            //performDatabaseCopy( connection, templateDatabaseName, targetDatabaseName, databaseType, userName, password, hostAndPort, adminDatabaseName);
            //copyTablesToSchema(connection, templateDatabaseName, targetDatabaseName, allTablesNames, unusedTablesNames);
            //result = copyContentToSchemaTables(connection, templateDatabaseName, targetDatabaseName, allTablesNames, unusedTablesNames);
        	List<String> tablesToCopy = removeElementsFromList(allTablesNames, unusedTablesNames);
            result = copyPostgreSQLSchemaToSQLFile(templateDatabaseName, pg_dumpPath, hostAndPort, userName, password);
        	createDatabasePostgresqlWithConnection(connection, targetDatabaseName);
            result = restoreDatabaseSchemaFromSQLFile(targetDatabaseName, hostAndPort, userName, password, psqlPath);
            Connection connectionCopy = getConnectionPostgreSQL(dbAddress, userName, password);
            result = copyPostgresqlTablesContent(connection, connectionCopy, templateDatabaseName, targetDatabaseName, tablesToCopy);
            /**
             * Dzia�a kopiowanie schematu bez danych do nowej bazy danych
             * TODO : kopiowanie danych do nowej bazy 
             * TODO : parametry wczytywane z linii polecen
             */
        }
        return result;
        
    }
    
    public static boolean restoreDatabaseSchemaFromSQLFile(String targetDatabaseName, String hostAndPort, String userName, String password, String psqlPath) {
    	boolean result = false;
    	
    	String host = getHostFromAddress(hostAndPort);
    	String port = getPortFromAddress(hostAndPort);
    	
        final List<String> baseCmds = new ArrayList<String>();
        baseCmds.add(psqlPath);
        baseCmds.add("-d");
        baseCmds.add(targetDatabaseName);
        baseCmds.add("-h");
        baseCmds.add(host);
        baseCmds.add("-p");
        baseCmds.add(port);
        baseCmds.add("-U");
        baseCmds.add(userName);
        //baseCmds.add("-v");
        baseCmds.add("-f");
        baseCmds.add(System.getProperty("user.home") + "/backup.sql");
   
        final ProcessBuilder pb = new ProcessBuilder(baseCmds);
        //psql -d database_name -h localhost -U postgres < path/db.sql
        
        // Set the password
        final Map<String, String> env = pb.environment();
        env.put("PGPASSWORD", password);

        try {
            final Process process = pb.start();

            final BufferedReader r = new BufferedReader(
                      new InputStreamReader(process.getErrorStream()));
            String line = r.readLine();
            while (line != null) {
                System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();
            removeFile(System.getProperty("user.home") + "/backup.sql");
            result = true;

         } catch (IOException e) {
            e.printStackTrace();
            removeFile(System.getProperty("user.home") + "/backup.sql");
         } catch (InterruptedException ie) {
            ie.printStackTrace();
            removeFile(System.getProperty("user.home") + "/backup.sql");
         }
    	
    	
    	
    	return result;
    }
    
    public static boolean copyPostgreSQLSchemaToSQLFile(String templateDatabaseName, String pg_dumpPath, String hostAndPort, String userName, String password ) {
    	boolean result = false;
    	
    	String host = getHostFromAddress(hostAndPort);
    	String port = getPortFromAddress(hostAndPort);
    	
        final List<String> baseCmds = new ArrayList<String>();
        baseCmds.add(pg_dumpPath);
        baseCmds.add("-h");
        baseCmds.add(host);
        baseCmds.add("-p");
        baseCmds.add(port);
        baseCmds.add("-U");
        baseCmds.add(userName);
        baseCmds.add("-s");
        baseCmds.add("-v");
        baseCmds.add("-f");
        baseCmds.add(System.getProperty("user.home") + "/backup.sql");
        baseCmds.add(templateDatabaseName);
        final ProcessBuilder pb = new ProcessBuilder(baseCmds);
        //pg_dump oldDB --schema masters  | psql -h localhost newDB;
        
        // Set the password
        final Map<String, String> env = pb.environment();
        env.put("PGPASSWORD", password);

        try {
            final Process process = pb.start();

            final BufferedReader r = new BufferedReader(
                      new InputStreamReader(process.getErrorStream()));
            String line = r.readLine();
            while (line != null) {
                System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();

         } catch (IOException e) {
            e.printStackTrace();
         } catch (InterruptedException ie) {
            ie.printStackTrace();
         } 
    	
    	//pg_dump -U pguser -s dvdrental > sqlPath
    	
    	/*final String cmd = "\"C:\\Program Files\\PostgreSQL\\9.5\\bin\\pg_dump \" dvdrental --schema skopiowana  | \"C:\\Program Files\\PostgreSQL\\9.5\\bin\\psql \" -h localhost:5432 newDB;";

        java.lang.Runtime rt = java.lang.Runtime.getRuntime();
        try {
			java.lang.Process p = rt.exec(cmd);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    	
    	
    	return result;
    }
    
    public static List<String> removeElementsFromList(List<String> names, List<String> unusedNames) {
    	for(int i = 0 ; i < unusedNames.size(); i++) {
    		if(names.contains(unusedNames.get(i))) {
    			names.remove(unusedNames.get(i));
    		}
    	}
    	return names;
    }
    
    public static String getPostgresqlCreateFunctionQuery(String templateDatabase, String targetDatabase) {
    	String query = "";
    	query += "CREATE OR REPLACE FUNCTION clone_schema(" + templateDatabase + " text, " + targetDatabase +" text) RETURNS void AS \n";
    	query += "$BODY$ \n";
    	query += "DECLARE \n";
    	query += "  objeto text; \n";
    	query += "  buffer text; \n";
    	query += "BEGIN \n";
    	query += "    EXECUTE 'CREATE SCHEMA IF NOT EXISTS ' || " + targetDatabase + " ; \n";
    	query += "    FOR objeto IN \n";
    	query += "        SELECT TABLE_NAME::text FROM information_schema.TABLES WHERE table_schema = " + templateDatabase + "\n";
    	query += "LOOP        \n";
    	query += "        buffer := " + targetDatabase +" || '.' || objeto; \n";
    	query += "        EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE ' || " + templateDatabase + " || '.' || objeto || ' INCLUDING ALL)'; \n";
    //	query += "        EXECUTE 'INSERT INTO ' || buffer || '(SELECT * FROM ' || " + templateDatabase + " || '.' || objeto || ')'; \n" ;
    	query += "    END LOOP; \n";
    	query += "END; \n";
    	query += "$BODY$ \n";
    	query += "LANGUAGE plpgsql VOLATILE;";	
    	return query;
    }
    
    public static void createMSSQLBackup(String templateDatabaseName, Connection connection ) {
    	String query = "";
    	createDatabaseMigrationDirectory();
    	query += "BACKUP DATABASE " + templateDatabaseName + " TO DISK = 'C:/DatabaseMigration/" + templateDatabaseName + ".bak'  \n";
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
            System.out.println("Error while executing.");
            e.printStackTrace();
            return;
        }
    }
    
    public static void restoreMssqlDatabaseFromZip(String databaseName, String adminDatabase, String userName, 
    		String password, String hostAndPort, String zipPath) {
    	
    	String dbAddress = hostAndPort + "/" + databaseName;
    	Connection connection = getConnectionMsSQL(dbAddress, userName, password, hostAndPort, adminDatabase, false);
    	//createMssqlDatabase(connection, databaseName);
    	restoreMssqlDatabase(connection, databaseName, zipPath);
    }
    
    public static void restoreMssqlDatabase(Connection connection, String databaseName, String zipPath) {
    	
    	createDatabaseMigrationDirectory();
    	unZip(zipPath, "C:/DatabaseMigration");
    	String query = "RESTORE DATABASE " + databaseName + " FROM DISK='C:/DatabaseMigration/" + databaseName + ".bak'  \n";
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
            removeFile("C:/DatabaseMigration/" + databaseName + ".bak");
            System.out.println("Database restored successfully.");
        }
        catch ( SQLException e )
        {
            System.out.println("Error while restoring database.");
            removeFile("C:/DatabaseMigration/" + databaseName + ".bak");
            e.printStackTrace();
            return;
        }
    }
    
    public static void createMssqlDatabase(Connection connection, String databaseName) {
    	String query = "CREATE DATABASE " + databaseName +";";
    	System.out.println(query);
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query); 
        }
        catch ( SQLException e )
        {
            System.out.println("Error while creating database.");
            e.printStackTrace();
            return;
        }
    }
    
    public static void createMSSQLBackupToNewDatabase(String templateDatabaseName, String targetDatabaseName, Connection connection) {
    	String query = "";
    	query += "BACKUP DATABASE " + templateDatabaseName + " TO DISK = 'C:/DatabaseMigration/" + templateDatabaseName + ".bak'  \n";
    	query += "RESTORE DATABASE " + targetDatabaseName + " FROM DISK='c:/DatabaseMigration/" + templateDatabaseName + ".bak'  \n";
    	query += "WITH  \n";
    	query += "MOVE 'trunk' TO 'c:/DatabaseMigration/" + targetDatabaseName + ".mdf', \n";
    	query += "MOVE 'trunk_log' TO 'c:/DatabaseMigration/" + targetDatabaseName + "_log.ldf ' \n";
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
            System.out.println("Error while executing.");
            e.printStackTrace();
            return;
        }
    	
    }
    
    public static void deleteDatabaseMigrationFile(String templateDatabaseName) {

    	try{		
    		File file = new File("c:/DatabaseMigration/" + templateDatabaseName + ".bak");
        	file.delete();
    	}catch(Exception e){	
    		e.printStackTrace();
    	}
    }
    
    
    public static void removeFile(String filePath) {
    	try{		
    		File file = new File(filePath);
        	file.delete();
    	}catch(Exception e){	
    		e.printStackTrace();
    	}
    }
    
	public static void createDatabaseMigrationDirectory() {
		File theDir = new File("C:/DatabaseMigration");

		if (!theDir.exists()) {
			boolean result = false;

			try {
				theDir.mkdir();
				result = true;
			} catch (SecurityException se) {
			}
		}
	}
    
    
    public static List<String> getDatabaseTablesNames(Connection connection) throws SQLException {
        List<String> tablesNames = new ArrayList<String>();
        DatabaseMetaData md = connection.getMetaData();
        ResultSet rs;
        
        String databaseType = md.getDatabaseProductName();
        if(databaseType.equals("PostgreSQL")) {
        	rs = md.getTables(null, null, "%", null);
        } else {
        	rs = md.getTables(null, "dbo", "%", null);
        }
        
        while (rs.next()) {
            if(rs.getString("TABLE_TYPE") != null) {
                if(rs.getString("TABLE_TYPE").equals("TABLE")) {
                    tablesNames.add(rs.getString("TABLE_NAME"));
                }   
            }
        }
        return tablesNames;

    }
    
    private static Connection getDatabaseConnection(String dbAdress, String userName, String password, String databaseType, String hostAndPort, String databaseName, boolean integratedSecurity) {
        Connection connection = null;
        if(databaseType.equals( "mssql") || databaseType.equals("mssql-integratedSecurity")) {
            connection = getConnectionMsSQL( dbAdress, userName, password, hostAndPort, databaseName, integratedSecurity);
        } else if(databaseType.equals( "postgresql" )) {
            connection = getConnectionPostgreSQL( dbAdress, userName, password );
        } else {
            System.out.println( "Wrong database type name" );
        }
        return connection;
    }
    
    private static Connection getConnectionMsSQL(String dbAdress, String userName, String password, String hostAndPort, String databaseName, boolean integratedSecurity) {
        try {
            //Class.forName("org.postgresql.Driver");
        	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {

            e.printStackTrace();
        }

        Connection connection = null;

        try {
        	String url = "";
        	if(integratedSecurity) {
        		url = "jdbc:sqlserver://" +  hostAndPort + ";DatabaseName=" + databaseName + ";integratedSecurity=true;";
        		connection = DriverManager.getConnection(url);
        	} else {
        		url = "jdbc:sqlserver://" +  hostAndPort + ";DatabaseName=" + databaseName;
        		connection = DriverManager.getConnection(url, userName, password);
        	}
        	
        	//String url = "jdbc:sqlserver://" +  hostAndPort + ";DatabaseName=" + databaseName; //+ ";integratedSecurity=true;";
        	System.out.println(url);
            //"jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE";
            // root password
        } catch (SQLException e) {
            
            System.out.println("Connection to: " + dbAdress + " failed.");
            e.printStackTrace();
        }
        return connection;
    }
    
    private static Connection getConnectionPostgreSQL(String dbAdress, String userName, String password) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {

            e.printStackTrace();
        }

        Connection connection = null;

        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql:" + dbAdress, userName, password);
            System.out.println(dbAdress);

        } catch (SQLException e) {
            
            System.out.println("Connection to: " + dbAdress + " failed.");
            e.printStackTrace();
        }
        return connection;
    }
    
    
    public static void exportPostgresToSQLFile(String pg_dumpPath, String host, String port, 
    		String userName, String password, String databaseName, String targetDirectoryPath) {
    	
        final List<String> baseCmds = new ArrayList<String>();
        baseCmds.add(pg_dumpPath);
        baseCmds.add("-h");
        baseCmds.add(host);
        baseCmds.add("-p");
        baseCmds.add(port);
        baseCmds.add("-U");
        baseCmds.add(userName);
        baseCmds.add("-b");
        baseCmds.add("-v");
        baseCmds.add("-f");
        baseCmds.add(targetDirectoryPath);
        baseCmds.add(databaseName);
        final ProcessBuilder pb = new ProcessBuilder(baseCmds);
        
        
        // Set the password
        final Map<String, String> env = pb.environment();
        env.put("PGPASSWORD", password);

        try {
            final Process process = pb.start();

            final BufferedReader r = new BufferedReader(
                      new InputStreamReader(process.getErrorStream()));
            String line = r.readLine();
            while (line != null) {
                System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();

         } catch (IOException e) {
            e.printStackTrace();
         } catch (InterruptedException ie) {
            ie.printStackTrace();
         }
    }
    
    public static void addDatabaseToZipArchive(String templateDatabaseName, String targetZipDirectoryPath) throws IOException {
    	  	
    	FileOutputStream fos;
		try {
			String filePath = System.getProperty("user.home") + File.separator + templateDatabaseName + ".backup";
			File file = new File(System.getProperty("user.home") + File.separator + templateDatabaseName + ".backup");
			fos = new FileOutputStream(targetZipDirectoryPath + ".zip");
			ZipOutputStream zos = new ZipOutputStream(fos);
			addToZipFile(filePath, zos);
			zos.close();
			fos.close();
			removeFile(filePath);
            System.out.println("Zip file created successfully.");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
            System.out.println("Exception while importing.");
		}
    }
    
    public static void addMssqlDatabaseToZipArchive(String databaseName, String targetZipDirectoryPath) throws IOException {
	  	
    	FileOutputStream fos;
		try {
			String filePath = "C:/DatabaseMigration/" + databaseName + ".bak";
			File file = new File("C:/DatabaseMigration/"+ databaseName + ".bak");
			fos = new FileOutputStream(targetZipDirectoryPath);
			ZipOutputStream zos = new ZipOutputStream(fos);
			addToZipFile(filePath, zos);
			zos.close();
			fos.close();
			removeFile(filePath);
            System.out.println("Zip file created successfully.");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
            System.out.println("Exception while importing.");
		}
    }
    
    public static void addToZipFile(String fileName, ZipOutputStream zos) throws FileNotFoundException, IOException {

		System.out.println("Writing '" + fileName + "' to zip file");

		File file = new File(fileName);
		FileInputStream fis = new FileInputStream(file);
		ZipEntry zipEntry = new ZipEntry(file.getName());
		zos.putNextEntry(zipEntry);

		byte[] bytes = new byte[1024];
		int length;
		while ((length = fis.read(bytes)) >= 0) {
			zos.write(bytes, 0, length);
		}

		zos.closeEntry();
		fis.close();
	}

    public static void restorePostgresqlDatabase(String psqlPath, String host, String port,
    		String userName, String password, String databaseName, String fullZipPath, String adminDatabaseName) {
    	
    	String dbAddress = "//" + host + "/" + adminDatabaseName;
    	createDatabasePostgresql(dbAddress, userName, password, databaseName);
    	unZip(fullZipPath, System.getProperty("user.home"));
    	String sqlPath = System.getProperty("user.home") +  File.separator + databaseName + ".backup";
    	
        final List<String> baseCmds = new ArrayList<String>();
        baseCmds.add(psqlPath);
        baseCmds.add("-h");
        baseCmds.add(host);
        baseCmds.add("-p");
        baseCmds.add(port);
        baseCmds.add("-U");
        baseCmds.add(userName);
        baseCmds.add("-d");
        baseCmds.add(databaseName);
        baseCmds.add("-f");
        baseCmds.add(sqlPath); 
        final ProcessBuilder pb = new ProcessBuilder(baseCmds);
        
        
        // Set the password
        final Map<String, String> env = pb.environment();
        env.put("PGPASSWORD", "pguser");

        try {
            final Process process = pb.start();

            final BufferedReader r = new BufferedReader(
                      new InputStreamReader(process.getErrorStream()));
            String line = r.readLine();
            while (line != null) {
                System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();
            System.out.println("Database imported successfully");
         } catch (IOException e) {
            e.printStackTrace();
         } catch (InterruptedException ie) {
            ie.printStackTrace();
         }
        removeFile(sqlPath); 
    }
    
    public static void unZip(String zipFile, String outputFolder){

        byte[] buffer = new byte[1024];
       	
        try{
       		
       	//create output directory is not exists
       	File folder = new File(outputFolder);
       	if(!folder.exists()){
       		folder.mkdir();
       	}
       		
       	//get the zip file content
       	ZipInputStream zis = 
       		new ZipInputStream(new FileInputStream(zipFile));
       	//get the zipped file list entry
       	ZipEntry ze = zis.getNextEntry();
       		
       	while(ze!=null){
       			
       	   String fileName = ze.getName();
              File newFile = new File(outputFolder + File.separator + fileName);
                   
              System.out.println("file unzip : "+ newFile.getAbsoluteFile());
                   
               //create all non exists folders
               //else you will hit FileNotFoundException for compressed folder
               new File(newFile.getParent()).mkdirs();
                 
               FileOutputStream fos = new FileOutputStream(newFile);             

               int len;
               while ((len = zis.read(buffer)) > 0) {
          		fos.write(buffer, 0, len);
               }
           		
               fos.close();   
               ze = zis.getNextEntry();
       	}
       	
           zis.closeEntry();
       	zis.close();
       		
       	System.out.println("Zip loaded.");
       		
       }catch(IOException ex){
          ex.printStackTrace(); 
       }
      } 
    
    public static void createDatabasePostgresql(String dbAdress, String userName, String password,
    		String databaseName) {
    	Connection connection = getConnectionPostgreSQL(dbAdress, userName, password);
    	String query = "CREATE DATABASE " + databaseName +";";
    	System.out.println(query);
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
            System.out.println("Error while creating database.");
            e.printStackTrace();
            return;
        }
    }
    
    public static void createDatabaseMsSQL(String targetDatabaseName, String templateDatabaseName, String dbAdress,
    		String userName, String password, boolean integratedSecurity, String hostAndPort) {
    	
    	Connection connection = getConnectionMsSQL(dbAdress, userName, password, hostAndPort, templateDatabaseName, integratedSecurity);
    	String query = "if db_id('" + targetDatabaseName +"') is null CREATE DATABASE " + targetDatabaseName +";";
    	System.out.println(query);
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
            System.out.println("Error while creating database.");
            e.printStackTrace();
            return;
        }
    }
    
    public static String getMsSQLCreateTableQuery(String tableName) {
    	String query = "";
    	query += "DECLARE @table_name SYSNAME \n";
    	query += "SELECT @table_name = 'dbo." + tableName + "' \n";
    	query += "DECLARE  \n";
    	query += "@object_name SYSNAME  \n";
    	query += ", @object_id INT  \n";
    	query += "SELECT   \n";
    	query += "@object_name = '[' + s.name + '].[' + o.name + ']'  \n";
    	query += ", @object_id = o.[object_id]  \n";
    	query += "FROM sys.objects o WITH (NOWAIT)  \n";
    	query += "JOIN sys.schemas s WITH (NOWAIT) ON o.[schema_id] = s.[schema_id]  \n";
    	query += "WHERE s.name + '.' + o.name = @table_name AND o.[type] = 'U' AND o.is_ms_shipped = 0  \n";
    	query += "DECLARE @SQL NVARCHAR(MAX) = ''  \n";
    	query += ";WITH index_column AS   \n";
    	query += "(  \n";
    	query += "SELECT  \n";
    	query += "ic.[object_id]  \n";
    	query += ", ic.index_id  \n";
    	query += ", ic.is_descending_key  \n";
    	query += ", ic.is_included_column  \n";
    	query += ", c.name  \n";
    	query += "FROM sys.index_columns ic WITH (NOWAIT)  \n";
    	query += "    JOIN sys.columns c WITH (NOWAIT) ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id  \n";
    	query += "WHERE ic.[object_id] = @object_id),  \n";
    	query += "fk_columns AS (  \n";
    	query += "SELECT k.constraint_object_id  \n";
    	query += ", cname = c.name  \n";
    	query += ", rcname = rc.name  \n";
    	query += "FROM sys.foreign_key_columns k WITH (NOWAIT)  \n";
    	query += "    JOIN sys.columns rc WITH (NOWAIT) ON rc.[object_id] = k.referenced_object_id AND rc.column_id = k.referenced_column_id   \n";
    	query += "    JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = k.parent_object_id AND c.column_id = k.parent_column_id  \n";
    	query += " WHERE k.parent_object_id = @object_id ) \n";
    	query += " SELECT @SQL = 'CREATE TABLE ' + @object_name + CHAR(13) + '(' + CHAR(13) + STUFF(( \n";
    	query += " SELECT CHAR(9) + ', [' + c.name + '] ' +  \n";
    	query += " CASE WHEN c.is_computed = 1 \n";
    	query += " THEN 'AS ' + cc.[definition] \n";
    	query += " ELSE UPPER(tp.name) +  \n";
    	query += "                 CASE WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text') \n";
    	query += "                        THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')' \n";
    	query += "                       WHEN tp.name IN ('nvarchar', 'nchar', 'ntext')\n";
    	query += "                         THEN '(' + CASE WHEN c.max_length = -1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'\n";
    	query += "  WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset')\n";
    	query += "  THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'\n";
    	query += "  WHEN tp.name = 'decimal' \n";
    	query += "                         THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ',' + CAST(c.scale AS VARCHAR(5)) + ')'\n";
    	query += "                      ELSE ''\n";
    	query += "                  END +\n";
    	query += "                  CASE WHEN c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END +\n";
    	query += "                  CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END +\n";
    	query += "                  CASE WHEN dc.[definition] IS NOT NULL THEN ' DEFAULT' + dc.[definition] ELSE '' END + \n";
    	query += "                  CASE WHEN ic.is_identity = 1 THEN ' IDENTITY(' + CAST(ISNULL(ic.seed_value, '0') AS CHAR(1)) + ',' + CAST(ISNULL(ic.increment_value, '1') AS CHAR(1)) + ')' ELSE '' END \n";
    	query += "          END + CHAR(13)\n";
    	query += "      FROM sys.columns c WITH (NOWAIT)\n";
    	query += "      JOIN sys.types tp WITH (NOWAIT) ON c.user_type_id = tp.user_type_id\n";
    	query += "      LEFT JOIN sys.computed_columns cc WITH (NOWAIT) ON c.[object_id] = cc.[object_id] AND c.column_id = cc.column_id\n";
    	query += "      LEFT JOIN sys.default_constraints dc WITH (NOWAIT) ON c.default_object_id != 0 AND c.[object_id] = dc.parent_object_id AND c.column_id = dc.parent_column_id\n";
    	query += "      LEFT JOIN sys.identity_columns ic WITH (NOWAIT) ON c.is_identity = 1 AND c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id\n";
    	query += "  WHERE c.[object_id] = @object_id\n";
    	query += "  ORDER BY c.column_id\n";
    	query += "  FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, CHAR(9) + ' ')\n";
    	query += "  + ISNULL((SELECT CHAR(9) + ', CONSTRAINT [' + k.name + '] PRIMARY KEY (' +\n";
    	query += "  (SELECT STUFF((\n";
    	query += "                           SELECT ', [' + c.name + '] ' + CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END\n";
    	query += "                           FROM sys.index_columns ic WITH (NOWAIT)\n";
    	query += "                           JOIN sys.columns c WITH (NOWAIT) ON c.[object_id] = ic.[object_id] AND c.column_id = ic.column_id\n";
    	query += "                           WHERE ic.is_included_column = 0\n";
    	query += "                              AND ic.[object_id] = k.parent_object_id \n";
    	query += "                               AND ic.index_id = k.unique_index_id  \n";
    	query += "                           FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, ''))\n";
    	query += "              + ')' + CHAR(13)\n";
    	query += "    FROM sys.key_constraints k WITH (NOWAIT)\n";
    	query += "              WHERE k.parent_object_id = @object_id \n";
    	query += "  AND k.[type] = 'PK'), '') + ')'  + CHAR(13)\n";
    	query += "  + ISNULL((SELECT (\n";
    	query += "  SELECT CHAR(13) +\n";
    	query += "  'ALTER TABLE ' + @object_name + ' WITH' \n";
    	query += "  + CASE WHEN fk.is_not_trusted = 1 \n";
    	query += "  THEN ' NOCHECK'\n";
    	query += "  ELSE ' CHECK'\n";
    	query += "  END +\n";
    	query += "  ' ADD CONSTRAINT [' + fk.name  + '] FOREIGN KEY('\n";
    	query += "  + STUFF((\n";
    	query += "  SELECT ', [' + k.cname + ']'\n";
    	query += "  FROM fk_columns k\n";
    	query += "  WHERE k.constraint_object_id = fk.[object_id]\n";
    	query += "  FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')\n";
    	query += "  + ')' +\n";
    	query += "                ' REFERENCES [' + SCHEMA_NAME(ro.[schema_id]) + '].[' + ro.name + '] ('\n";
    	query += "                + STUFF((\n";
    	query += "                  SELECT ', [' + k.rcname + ']'\n";
    	query += "                  FROM fk_columns k\n";
    	query += "                  WHERE k.constraint_object_id = fk.[object_id]\n";
    	query += "                  FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '')\n";
    	query += "  + ')'\n";
    	query += "  + CASE\n";
    	query += "                  WHEN fk.delete_referential_action = 1 THEN ' ON DELETE CASCADE' \n";
    	query += "                  WHEN fk.delete_referential_action = 2 THEN ' ON DELETE SET NULL'\n";
    	query += "  WHEN fk.delete_referential_action = 3 THEN ' ON DELETE SET DEFAULT'\n";
    	query += "  ELSE ''\n";
    	query += "  END\n";
    	query += "  + CASE \n";
    	query += "  WHEN fk.update_referential_action = 1 THEN ' ON UPDATE CASCADE'\n";
    	query += "  WHEN fk.update_referential_action = 2 THEN ' ON UPDATE SET NULL'\n";
    	query += "  WHEN fk.update_referential_action = 3 THEN ' ON UPDATE SET DEFAULT' \n";
    	query += "  ELSE ''\n";
    	query += "   END\n";
    	query += "              + CHAR(13) + 'ALTER TABLE ' + @object_name + ' CHECK CONSTRAINT [' + fk.name  + ']' + CHAR(13)\n";
    	query += "          FROM sys.foreign_keys fk WITH (NOWAIT)\n";
    	query += "          JOIN sys.objects ro WITH (NOWAIT) ON ro.[object_id] = fk.referenced_object_id\n";
    	query += "          WHERE fk.parent_object_id = @object_id\n";
    	query += "          FOR XML PATH(N''), TYPE).value('.', 'NVARCHAR(MAX)')), '')\n";
    	query += "      + ISNULL(((SELECT\n";
    	query += "           CHAR(13) + 'CREATE' + CASE WHEN i.is_unique = 1 THEN ' UNIQUE' ELSE '' END \n";
    	query += "                  + ' NONCLUSTERED INDEX [' + i.name + '] ON ' + @object_name + ' (' +\n";
    	query += "                  STUFF((\n";
    	query += "                  SELECT ', [' + c.name + ']' + CASE WHEN c.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END\n";
    	query += "                  FROM index_column c\n";
    	query += "   WHERE c.is_included_column = 0\n";
    	query += "                      AND c.index_id = i.index_id\n";
    	query += "                  FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')'  \n";
    	query += "                  + ISNULL(CHAR(13) + 'INCLUDE (' + \n";
    	query += "                      STUFF((\n";
    	query += "                      SELECT ', [' + c.name + ']'\n";
    	query += "  FROM index_column c\n";
    	query += "                      WHERE c.is_included_column = 1\n";
    	query += "  AND c.index_id = i.index_id\n";
    	query += "                      FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')', '')  + CHAR(13)\n";
    	query += "          FROM sys.indexes i WITH (NOWAIT)\n";
    	query += "  WHERE i.[object_id] = @object_id\n";
    	query += "  AND i.is_primary_key = 0\n";
    	query += "  AND i.[type] = 2\n";
    	query += "          FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)')\n";
    	query += "  ), '')\n";
    	query += "  SELECT @SQL\n";
    	query += "  \n";
    	
    	return query;
    }
    
    public static String getMsSQLCreateTablesQuery() {
    	String query = "";
    	query += "select  'create table [' + so.name + '] (' + o.list + ')' + CASE WHEN tc.Constraint_Name IS NULL THEN '' ELSE 'ALTER TABLE ' + so.Name + ' ADD CONSTRAINT ' + tc.Constraint_Name  + ' PRIMARY KEY ' + ' (' + LEFT(j.List, Len(j.List)-1) + ')' END  \n";
    	query += "from    sysobjects so  \n";
    	query += "cross apply  \n";
    	query += "(SELECT   \n";
    	query += "'  ['+column_name+'] ' +  \n";
    	query += "        data_type + case data_type  \n";
    	query += "when 'sql_variant' then ''  \n";
    	query += "when 'text' then ''  \n";
    	query += "when 'ntext' then ''  \n";
    	query += "when 'xml' then ''  \n";
    	query += "            when 'decimal' then '(' + cast(numeric_precision as varchar) + ', ' + cast(numeric_scale as varchar) + ')'  \n";
    	query += "            else coalesce('('+case when character_maximum_length = -1 then 'MAX' else cast(character_maximum_length as varchar) end +')','') end + ' ' +  \n";
    	query += "case when exists (  \n";
    	query += "select id from syscolumns  \n";
    	query += "where object_name(id)=so.name  \n";
    	query += "and name=column_name  \n";
    	query += "and columnproperty(id,name,'IsIdentity') = 1  \n";
    	query += ") then  \n";
    	query += "'IDENTITY(' +  \n";
    	query += "cast(ident_seed(so.name) as varchar) + ',' +  \n";
    	query += "cast(ident_incr(so.name) as varchar) + ')'  \n";
    	query += "else '' \n";
    	query += "end + ' ' +  \n";
    	query += "         (case when IS_NULLABLE = 'No' then 'NOT ' else '' end ) + 'NULL ' +   \n";
    	query += "          case when information_schema.columns.COLUMN_DEFAULT IS NOT NULL THEN 'DEFAULT '+ information_schema.columns.COLUMN_DEFAULT ELSE '' END + ', '   \n";
    	query += " from information_schema.columns where table_name = so.name  \n";
    	query += " order by ordinal_position \n";
    	query += " FOR XML PATH('')) o (list) \n";
    	query += " left join \n";
    	query += "  information_schema.table_constraints tc \n";
    	query += " on  tc.Table_name       = so.Name  \n";
    	query += "AND tc.Constraint_Type  = 'PRIMARY KEY'  \n";
    	query += "cross apply  \n";
    	query += "    (select '[' + Column_Name + '], '  \n";
    	query += "FROM   information_schema.key_column_usage kcu  \n";
    	query += "WHERE  kcu.Constraint_Name = tc.Constraint_Name  \n";
    	query += " ORDER BY  \n";
    	query += " ORDINAL_POSITION \n";
    	query += " FOR XML PATH('')) j (list) \n";
    	query += " where   xtype = 'U' \n";
    	query += " AND name    NOT IN ('dtproperties') \n";

    	
    	return query;
    }
    
    public static void copyMsSQLTablesSchemaToTargetDatabase(Connection connection, Connection connectionCopy) {
    	try {
			List<String> tablesNames = getDatabaseTablesNames(connection);
				String query = getMsSQLCreateTablesQuery();
				
				Statement statement = connection.createStatement();
				Statement statementCopy = connectionCopy.createStatement();

				ResultSet rs = statement.executeQuery(query);
				while(rs.next()) {
					String createTableQuery = rs.getString(1);
				    System.out.println(createTableQuery);
					statementCopy.executeUpdate(createTableQuery);
				}

		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
    
    public static boolean copyMsSQLTablesContent(Connection connectionCopy, String templateDatabaseName,
    	List<String> tablesToCopy) {
    	boolean result = false;
    	for(int i = 0; i < tablesToCopy.size(); i++) {
    		String query = getMsSQLInsertStatement(tablesToCopy.get(i), templateDatabaseName);
    		System.out.println(query);
    		try {
				Statement statement = connectionCopy.createStatement();
				statement.executeUpdate(query);
				result = true;
			} catch (SQLException e) {
				result = false;
				e.printStackTrace();
			}
    	}   	
    	return result;
    }
    
    public static String getMsSQLInsertStatement(String targetTableName, String templateDatabaseName) {
    	String query = "INSERT INTO [" + targetTableName + "] SELECT * FROM [" + templateDatabaseName +"].[dbo].[" + targetTableName +"]";
    	return query;
    }
    
    public static void createDatabasePostgresqlWithConnection(Connection connection, String databaseName) {
    	String query = "CREATE DATABASE " + databaseName +";";
    	System.out.println(query);
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
            System.out.println("Error while creating database.");
            e.printStackTrace();
            return;
        }
    }
    
    public static boolean copyPostgresqlTablesContent(Connection connection, Connection connectionCopy, String templateDatabaseName,
    		String targetDatabaseName, List<String> tablesToCopy) {
    	
    	String tableName;
    	System.out.println("Inserting data to selected tables...");
    	boolean result = false;
    	
    	
    	List<String> tables = new ArrayList<>();
    	Statement statement;
    	Statement statementCopy;
		try {
			statement = connection.createStatement();
			statementCopy = connectionCopy.createStatement();
			
			tables = collectTablesList(tablesToCopy, connectionCopy);
			System.out.println( "" );
			for(int k = 0 ; k < tables.size(); k++) {
				System.out.println(tables.get(k));
			}
			
			for(int i = 0; i < tables.size(); i++) {
	    		tableName = tables.get(i);
	    		ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
	    		
	    		
	    		String insert = prepareInsertQueryForTable(rs, tableName);
	    		
	    		List<PreparedStatement> preparedStatements = prepareInsertStatement(insert, rs, connectionCopy);
	    		for(int j = 0 ; j < preparedStatements.size(); j++) {
	    		    System.out.println( preparedStatements.get( j ) );
	    			preparedStatements.get(j).execute();
	    		}	
	    	}
			result = true;
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}    	
    	return result;
    }
    
    public static List<String> collectTablesList(List<String> tablesNames, Connection connection) {
    	List<String> finalTablesOrder = new ArrayList<>();
    	List<String> tablesWithForeignKeys = new ArrayList<>();
    	List<String> normalTables = new ArrayList<>();
    	
    	for(int i = 0; i < tablesNames.size(); i++) {
    		String table = tablesNames.get(i);
    		if(chceckIfTableContainsForeignKey(table, connection)) {
    		   // System.out.println( "FK TABLE: " + table );
    			tablesWithForeignKeys.add(table);
    		} else {
    		    //System.out.println( "TABLE: " + table );
    			normalTables.add(table);
    		}
    	}
    	
		boolean endLoop = false;
		int counter = 0;
        int tmpCounter = 0;
		do {
		    
		    System.out.println(normalTables.size());
		    tmpCounter = normalTables.size();
		   
		    for(int i = 0; i < tablesWithForeignKeys.size(); i++) {
		        
		        String tableName = tablesWithForeignKeys.get(i);
		        List<String> foreignsKeys = getForeignKeysForTable( tableName, connection );
		        
		        
		        if(checkIfListContainsAllElements( foreignsKeys, normalTables )) {
		            normalTables.add( tableName );
		            tablesWithForeignKeys.remove( tableName );
		            counter = normalTables.size();
		        }
		    }
		    
		    if(tmpCounter == counter) {
	              for(int k = 0; k < tablesWithForeignKeys.size(); k++) {
	                    normalTables.add(tablesWithForeignKeys.get(k));
	                }
		        endLoop = true;
		    }
		   
		    
		   /* System.out.println( "Normal tables : ---------------------------------------------" );
            for(int j = 0; j < normalTables.size(); j++) {
                System.out.println( normalTables.get( j ) );
            }
            
            System.out.println( "Foreign Keys tables : ---------------------------------------------" );
            for(int j = 0; j < tablesWithForeignKeys.size(); j++) {
                System.out.println( tablesWithForeignKeys.get( j ) );
            }
            
            System.out.println( "Foreign keys : ---------------------------------------------" );
            List<String> tempKeys = new ArrayList<>();
            
            for(int j = 0; j < tablesWithForeignKeys.size(); j++) {
               // System.out.println( tablesWithForeignKeys.get( j ) );
                List<String> tmp = getForeignKeysForTable( tablesWithForeignKeys.get( j ), connection ) ;
                for(int k = 0; k < tmp.size();k++) {
                    tempKeys.add( tmp.get( k ) );
                }
            }
            
            for(int j = 0; j < tempKeys.size(); j++) {
                System.out.println( tempKeys.get( j ) );
            }  */
		    
		    if(normalTables.size() == tablesNames.size()) {
		        endLoop = true;
		    }
		    
		} while(!endLoop);
		
		
		return normalTables;

    }
    
    
    public static boolean checkIfListContainsAllElements(List<String> elements, List<String> list) {
        for(int i = 0; i < elements.size(); i++) {
            if(!list.contains( elements.get( i ) )) {
                return false;
            }
        }
        return true;
    }
    
    public static String getFinalTable(List<String> dirtyTables, List<String> normalTables, String dirtyTable, Connection connection) {
        
        boolean allKeysInDirtyTables = false;
        List<String> foreignKeys = getForeignKeysForTable(dirtyTable, connection);
        boolean[] checkArray = new boolean[foreignKeys.size()];
        
        for(int i = 0; i < checkArray.length; i++) {
            checkArray[i] = false;
        }
        
        for(int i = 0 ; i < foreignKeys.size(); i++) {
            if(normalTables.contains(foreignKeys.get(i))) {
                checkArray[i] = true;
            }
        }
        
        for(int i = 0; i < foreignKeys.size(); i++) {
            if(dirtyTables.contains(foreignKeys.get(i))) {
                
                int secondPosition = dirtyTables.indexOf(foreignKeys.get(i));
                String secondTable = dirtyTables.get(secondPosition);
                List<String> secondForeignKeys = getForeignKeysForTable(secondTable, connection);
                boolean[] checkArraySecond = new boolean[secondForeignKeys.size()];
                
                for(int j = 0; j < checkArraySecond.length; j++) {
                    checkArraySecond[j] = false;
                }
                
                for(int j = 0 ; j < secondForeignKeys.size(); j++) {
                    if(normalTables.contains(secondForeignKeys.get(j))) {
                        checkArraySecond[j] = true;
                    }
                }
                
                
                
                for(int j = 0; j < checkArraySecond.length; j++) {
                    if(!checkArray[i]) {
                        System.out.println( secondTable );
                        getFinalTable(dirtyTables, normalTables, secondTable, connection);
                    }
                }
                
                checkArray[i] = true;
            }
        }
        
        for(int i = 0; i < checkArray.length; i++) {
            if(!checkArray[i]) {
                //dirtyTables.remove( dirtyTable );
                int dirtyTablePosition = dirtyTables.indexOf( dirtyTable );
                dirtyTable = dirtyTables.get( dirtyTablePosition + 1 );
                System.out.println( "YYYYYYYYYY" );
                getFinalTable(dirtyTables, normalTables, dirtyTable, connection);
            }
        }
        return dirtyTable;
    }
    
   public static List<String> getForeignKeysForTable(String tableName, Connection connection) {
    	Statement statement;
    	List<String> foreignKeys = new ArrayList<>();
		try {
			statement = connection.createStatement();
			DatabaseMetaData dm = connection.getMetaData();
			ResultSet tableForeignKeys = dm.getExportedKeys( null, null, tableName );
			ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName + " LIMIT 1");
			ResultSetMetaData rsmd = rs.getMetaData();
			//ResultSet keysRs = dm.getImportedKeys(null, null, tableName);
			String column = "";

			while (tableForeignKeys.next()) {
			    String column_name = tableForeignKeys.getString("FKTABLE_NAME");
		        foreignKeys.add(column_name);
		    }
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
		/*System.out.println( "Klucze obce tabeli: " + tableName);
		for(int i = 0; i < foreignKeys.size(); i++) {
		    System.out.println( foreignKeys.get( i ) );
		}
		System.out.println( "" );*/
		return foreignKeys;
    } 
    
    public static boolean chceckIfTableContainsForeignKey(String tableName, Connection connection) {
    	boolean result = false;
    	try {
			Statement statement = connection.createStatement();
			DatabaseMetaData dm = connection.getMetaData();
			ResultSet tableForeignKeys = dm.getImportedKeys(null, null, tableName );
			String column_name = "";
            while ( tableForeignKeys.next() )
            {
                column_name = tableForeignKeys.getString( "FKTABLE_NAME" );
            }
			if(!column_name.equals("")) {
			    result = true;
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
    	return result;
    }
    
    public static List<PreparedStatement> prepareInsertStatement(String query, ResultSet rs, Connection connectionCopy) {
    	
    	List<PreparedStatement> preparedSatatements = new ArrayList<PreparedStatement>();
    	PreparedStatement pstmt = null;
    	try {
			ResultSetMetaData metaData = rs.getMetaData();
			
			while (rs.next()) {
				pstmt = connectionCopy.prepareStatement(query);
				for(int i = 1 ; i <= metaData.getColumnCount() ; i++) {
			
                    Object str = rs.getObject(i);
                    pstmt.setObject( i, str);
					//pstmt.setString(i, str);
				}
				preparedSatatements.add(pstmt);
			}
			
			
		} catch (SQLException e) {
		
			e.printStackTrace();
		}
    	
    	return preparedSatatements;
    }
    
    public static String prepareInsertQueryForTable(ResultSet rs, String tableName) {
    	StringBuilder columnNames = new StringBuilder();
		StringBuilder bindVariables = new StringBuilder();
		int columnCount;
		try {
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				if (i > 1) {
					columnNames.append(", ");
					bindVariables.append(", ");
				}

				columnNames.append(metaData.getColumnName(i));
				bindVariables.append('?');
			}

		} catch (SQLException e) {

			e.printStackTrace();
		}	
		   String sql = "INSERT INTO " + tableName + " ("
		              + columnNames
		              + ") VALUES ("
		              + bindVariables
		              + ");";
    	return sql;
    }

}
