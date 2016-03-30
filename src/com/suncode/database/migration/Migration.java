package com.suncode.database.migration;
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
import java.time.YearMonth;


/**
 * @author adamszczesiak 23 mar 2016
 */
public class Migration
{
    /**
     * 
     * @param args:  postgresql/mssql/mssql-integratedSecurity server:port templateDBName targetDBName user password unusedTables adminDBName
     * @arg[0]: export-postgresql/export-mssql/import-postgresql/import-mssql 
     * 
     * export-postgresql targetZipDirectory pg_dumpPath host port userName password databaseName
     * export-postgresql C:\\Users\\Adam\\Desktop\\ "C:\\Program Files\\PostgreSQL\\9.5\\bin\\pg_dump" localhost 5432 pguser pguser dvdrental
     * import-postgresql "C:\\Program Files\\PostgreSQL\\9.5\\bin\\psql.exe", localhost, 5432, pguser, dvdrental, C://Users//Adam//Desktop//dvdrental.zip
     * @throws SQLException
     * @throws ClassNotFoundException
     * pg_dump path : C:\\Program Files\\PostgreSQL\\9.5\\bin\\pg_dump
     * @throws IOException 
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
                		port, userName, password, databaseName, System.getProperty("user.home") + "\\" + databaseName + ".backup");
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
            
            String databaseType = args[0];
            String dbAdress = "//" + args[1];     //"localhost:5432";
            String templateDatabaseName = args[2];
            String targetDatabaseName = args[3];
            String userName = "";
            String password = "";
            if(!integratedSecurity) {
                userName = args[4];    //"postgres";
                password = args[5];    //"postgres";
            }

            String dbAdressCopy = dbAdress + "/" + args[3];
            String adminDatabaseAddress = "//" + args[1] + "/" + args[6];
            dbAdress += "/" + args[2];
            
            String hostAndPort = args[1];
            String adminDatabaseName = args[6];
            
            tablesNames = getArgsTablesNames(args, integratedSecurity);          
                   
            Connection connection = getDatabaseConnection(dbAdress, userName, password, databaseType, hostAndPort, templateDatabaseName, integratedSecurity);
            
            List<String> allTablesNames = getDatabaseTablesNames(connection);
            result = copySchema(connection, templateDatabaseName, targetDatabaseName, userName, password, hostAndPort, adminDatabaseName, allTablesNames,tablesNames);
                        
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
    
    public static List<String> getArgsTablesNames(String[] args, boolean integratedSecurity) {
    	List<String> tablesNames = new ArrayList<String>();
    	if(integratedSecurity) {
    		for(int i = 4; i < args.length; i++) {
                tablesNames.add(args[i]);
            }
    	} else {
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
    
    public static boolean deleteUnusedTables(Connection connection, String targetDatabaseName, List<String> unusedTablesNames, String databaseType) {
        boolean result = false;
        try
        {
            List<String> databaseTables = getDatabaseTablesNames(connection);
            List<String> wrongTablesNames = checkDatabaseTablesNames(databaseTables, unusedTablesNames);
            if(!wrongTablesNames.isEmpty()) {
                System.out.print("Selected tables does not exist : ");
                for(int i = 0; i < wrongTablesNames.size(); i++) {
                    System.out.print(" " + wrongTablesNames.get(i));
                }
                System.out.println("");
                deleteDatabase(connection, targetDatabaseName);
                result = false;
            } else {
                if(!unusedTablesNames.isEmpty()) {
                    try
                    {
                        Statement statement = connection.createStatement();
                        String query = prepareDeleteTablesQuery(unusedTablesNames, databaseType, targetDatabaseName);
                        if(databaseType.equals("mssql") || databaseType.equals("mssql-integratedSecurity")) {
                        	deleteConstraintsForMSSQLTables(connection, unusedTablesNames, targetDatabaseName);
                        }
                        statement.executeUpdate(query);
                        result = true;
                    }
                    catch ( SQLException e )
                    {
                        e.printStackTrace();
                        deleteDatabase(connection, targetDatabaseName);
                    }            
                } else {
                	result = true;
                }
            }
        }
        catch ( SQLException e1 )
        {
            e1.printStackTrace();
        }
         

        return result;
    }
    
    public static void deleteDatabase(Connection connection, String databaseName) {
        Statement statement;
        try
        {
            statement = connection.createStatement();
            String query = "DROP DATABASE " + databaseName + ";";
            statement.executeUpdate(query);
            connection.close();
        }
        catch ( SQLException e )
        {
            e.printStackTrace();
        }

    }
    
    public static void deleteConstraintsForMSSQLTables(Connection connection, List<String> unusedTablesNames, String targetDatabaseName) {
    	for(int i = 0; i < unusedTablesNames.size(); i++) {
    		String tableName = unusedTablesNames.get(i);
    		String removeConstraintsQuery = prepareRemoveConstraintsQuery(targetDatabaseName, tableName);
    		Statement statement;
            try
            {
                statement = connection.createStatement();
                statement.executeUpdate(removeConstraintsQuery);
            }
            catch ( SQLException e )
            {
                e.printStackTrace();
            }
    	}
    }
    
    public static String prepareRemoveConstraintsQuery(String targetDatabaseName, String tableName) {
    	String query = "";
    	query += "DECLARE @database nvarchar(50) \n";
    	query += "DECLARE @table nvarchar(50) \n";
    	query += "set @database = '" + targetDatabaseName +"' \n";
    	query += "set @table = '" + tableName + "' \n";
    	query += "DECLARE @sql nvarchar(255) \n";
    	query += "WHILE EXISTS(select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS where constraint_catalog = @database and table_name = @table) \n";
    	query += "BEGIN \n";
    	query += "select    @sql = 'ALTER TABLE ' + @table + ' DROP CONSTRAINT ' + CONSTRAINT_NAME  \n";
    	query += "from    INFORMATION_SCHEMA.TABLE_CONSTRAINTS \n";
    	query += "where    constraint_catalog = @database and \n";
    	query += "table_name = @table \n";
    	query += "exec    sp_executesql @sql \n";
    	query += "END";
    	
    	return query;
    }
    
    
    public static String prepareDeleteTablesQuery(List<String> unusedTablesNames, String databaseType, String targetDatabaseName) {
        String query = "DROP TABLE ";
        System.out.println(unusedTablesNames.size());
        for(int i = 0; i < unusedTablesNames.size(); i++) {
            if(i != unusedTablesNames.size()-1) {
            	
            	if(databaseType.equals("postgresql")) {
            		query += unusedTablesNames.get(i) +",";
            	} else if(databaseType.equals("mssql") || databaseType.equals("mssql-integratedSecurity")) {
            		query += "[" + targetDatabaseName + "].[dbo].[" + unusedTablesNames.get(i) + "]"; 
            		//[trunk].[dbo].[User]		
            	}          
            } else {
            	if(databaseType.equals("postgresql")) {
            		//query += unusedTablesNames.get(i) +" CASCADE;";
            		query += unusedTablesNames.get(i) +" ;";
            	} else if(databaseType.equals("mssql") || databaseType.equals("mssql-integratedSecurity")) {
            		query += "[" + targetDatabaseName + "].[dbo].[" + unusedTablesNames.get(i) + "]" +";";
            	}   
            }
        }
        System.out.println(query);
        return query;
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
    		String userName, String password, String dbAddress, String adminDatabaseName, List<String> allTablesNames,
    		List<String> unusedTablesNames) throws SQLException {
        boolean result = false;
        DatabaseMetaData metaData = connection.getMetaData();
        String databaseType = metaData.getDatabaseProductName();
        if(databaseType.equals("Microsoft SQL Server")) {
        	createDatabaseMigrationDirectory();
        } else if(databaseType.equals("PostgreSQL")) {
            performDatabaseCopy( connection, templateDatabaseName, targetDatabaseName, databaseType, userName, password, dbAddress, adminDatabaseName);
            copyTablesToSchema(connection, templateDatabaseName, targetDatabaseName, allTablesNames, unusedTablesNames);
            result = copyContentToSchemaTables(connection, templateDatabaseName, targetDatabaseName, allTablesNames, unusedTablesNames);
            //createDatabaseWithTemplate(connection, targetDatabaseName,templateDatabaseName , templateDatabaseName);
        }
        return result;
        
    }
    
    public static void performDatabaseCopy(Connection connection, String templateDatabaseName, String targetDatabaseName, 
    		String databaseType, String userName, String password, String dbAddress, String adminDatabaseName) {
        
    	String adminDatabaseAddress =  "//" + dbAddress + "/"  + adminDatabaseName;
    	createSchema(connection, targetDatabaseName);
    	
        String query = "";
        if(databaseType.equals( "PostgreSQL" )) {
        	String createFunctionQuery = getPostgresqlCreateFunctionQuery(templateDatabaseName, targetDatabaseName);
            Statement statement;
            try
            {
                statement = connection.createStatement();
                //statementInvoke = connection.createStatement();
                statement.execute(createFunctionQuery);
            }
            catch ( SQLException e )
            {
                System.out.println("Error while creating database: " + targetDatabaseName);
                e.printStackTrace();
                return;
            }
                      
        } else if (databaseType.equals("Microsoft SQL Server")) {
           createMSSQLBackupToNewDatabase(templateDatabaseName, targetDatabaseName, connection);
           deleteDatabaseMigrationFile(templateDatabaseName);
        }              
    }
    
    public static boolean copyContentToSchemaTables(Connection connection, String templateDatabaseName, 
    		String targetDatabaseName, List<String> allTablesNames, List<String> unusedTablesNames) {
    	
    	boolean result;
    	Statement statement;
    	allTablesNames = removeElementsFromList(allTablesNames, unusedTablesNames);
    	
        try {
			statement = connection.createStatement();
	    	for(int i = 0; i < allTablesNames.size(); i++) {
	    		String tableName = allTablesNames.get(i);
	    		statement.execute("INSERT INTO " + targetDatabaseName +"." + tableName + " (SELECT * FROM " + "public." + tableName + " );");
	    	}
		} catch (SQLException e) {
			e.printStackTrace();
		}
        result = true;
        return result;
    }
    
    public static void copyTablesToSchema(Connection connection, String templateDatabaseName, String targetDatabaseName, List<String> allTablesNames, List<String> unusedTablesNames) {
    	Statement statement;
        try {
			statement = connection.createStatement();
			
	    	for(int i = 0; i < allTablesNames.size(); i++) {
	    		String tableName = allTablesNames.get(i);
	    		statement.execute("CREATE TABLE IF NOT EXISTS "+ targetDatabaseName + "." + tableName + " (LIKE " +  "public." + tableName + " INCLUDING ALL);");
	    	}
		} catch (SQLException e) {
			e.printStackTrace();
		}
    }
    
    public static List<String> removeElementsFromList(List<String> names, List<String> unusedNames) {
    	for(int i = 0 ; i < unusedNames.size(); i++) {
    		if(names.contains(unusedNames.get(i))) {
    			names.remove(unusedNames.get(i));
    		}
    	}
    	return names;
    }
    
    public static void createSchema(Connection connection, String schemaName) {
    	String query = "CREATE SCHEMA IF NOT EXISTS " + schemaName +";";
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
    
    public static void createTablesAndCopyContentOfPostgreSQL(Connection connection, Connection templateConnection, String templateDatabaseName, List<String> unusedTablesNames) throws SQLException {
    	List<String> tablesNames = getDatabaseTablesNames(templateConnection);
    	String query = "";
    	query += "CREATE TABLE " + tablesNames.get(0) + " (LIKE " + tablesNames.get(0) + " INCLUDING ALL);";
    	/*for(int i = 0; i < tablesNames.size(); i++) {
    		if(unusedTablesNames.contains(tablesNames.get(i))) {
    			query += "CREATE TABLE " + tablesNames.get(i) + " (LIKE " + templateDatabaseName +".public." + tablesNames.get(i) + " INCLUDING ALL);";
    		} else {
    			query += "CREATE TABLE " + tablesNames.get(i) + " (LIKE " + templateDatabaseName +".public." + tablesNames.get(i) + " INCLUDING ALL);";
    			query += "INSERT INTO " + tablesNames.get(i) + " SELECT * FROM" + templateDatabaseName +".public." + tablesNames.get(i)+ ";";
    		}
    	}*/
    	
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
    
    public static void createMSSQLBackup(String templateDatabaseName, Connection connection ) {
    	String query = "";
    	createDatabaseMigrationDirectory();
    	query += "BACKUP DATABASE " + templateDatabaseName + " TO DISK = 'C:\\DatabaseMigration\\" + templateDatabaseName + ".bak'  \n";
    	
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
    	unZip(zipPath, "C:\\DatabaseMigration");
    	
    	String query = "RESTORE DATABASE " + databaseName + " FROM DISK='c:\\DatabaseMigration\\" + databaseName + ".bak'  \n";
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
    	query += "BACKUP DATABASE " + templateDatabaseName + " TO DISK = 'C:\\DatabaseMigration\\" + templateDatabaseName + ".bak'  \n";
    	query += "RESTORE DATABASE " + targetDatabaseName + " FROM DISK='c:\\DatabaseMigration\\" + templateDatabaseName + ".bak'  \n";
    	query += "WITH  \n";
    	query += "MOVE 'trunk' TO 'c:\\DatabaseMigration\\" + targetDatabaseName + ".mdf', \n";
    	query += "MOVE 'trunk_log' TO 'c:\\DatabaseMigration\\" + targetDatabaseName + "_log.ldf ' \n";
    	
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
    		File file = new File("c:\\DatabaseMigration\\" + templateDatabaseName + ".bak");
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
			System.out.println("creating directory: " + "C:/DatabaseMigration");
			boolean result = false;

			try {
				theDir.mkdir();
				result = true;
			} catch (SecurityException se) {
			}
		}
	}
    
    public static void copyDatabaseContent(String userName, String password,  String templateDatabaseName, String targetDatabaseName) throws SQLException {
        //mysqldump -u <user name> -p <pwd> <original db> | mysql -u <user name> <pwd> <new db>
        final String cmd = " mysqldump -u " + userName + " -p " + password + " " + templateDatabaseName + " | mysql -u " + userName +  " -p " + password + " " + targetDatabaseName +";";
        
       // mysqldump -u admin -p originaldb | mysql -u backup -pPassword duplicateddb;
        System.out.println( cmd );
        
        java.lang.Runtime rt = java.lang.Runtime.getRuntime();
        try
        {
            java.lang.Process p = rt.exec(cmd);
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }      
    }
    
    public static String getValuesString(ResultSetMetaData resultMetaData) throws SQLException {
        //VALUES (?,?,?)
        int counter = resultMetaData.getColumnCount();
        String values = " VALUES (";
        for(int i = 0 ; i < counter; i++) {
            if(i != counter-1) {
                values += "?,";
            } else {
                values += "?);";
            }
        }
        return values;
    }
    
    
    public static List<String> getDatabaseTablesNames(Connection connection) throws SQLException {
        List<String> tablesNames = new ArrayList<String>();
        DatabaseMetaData md = connection.getMetaData();
        ResultSet rs = md.getTables(null, null, "%", null);
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
			String filePath = System.getProperty("user.home") + "\\" + templateDatabaseName + ".backup";
			File file = new File(System.getProperty("user.home") + "\\" + templateDatabaseName + ".backup");
			fos = new FileOutputStream(targetZipDirectoryPath + templateDatabaseName + ".zip");
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
			String filePath = "C:\\DatabaseMigration\\" + databaseName + ".bak";
			File file = new File("C:\\DatabaseMigration\\"+ databaseName + ".bak");
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
    	String sqlPath = System.getProperty("user.home") + "\\" + databaseName + ".backup";
    	
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
    
}
