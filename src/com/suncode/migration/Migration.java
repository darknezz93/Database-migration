package com.suncode.migration;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.sound.sampled.TargetDataLine;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.SQLExec;

import java.util.Properties;

import java.sql.Types;


/**
 * @author adamszczesiak 23 mar 2016
 */
public class Migration
{
    /**
     * 
     * 
     * CLONE EXAMPLES:
     * 
     * (nazwy tabel z drugiej bazy danych w merge zapisane w pliku konfiguracyjnym mergeTables.properties)
     * (nazwy tabel z drugiej bazy danych w merge zapisane w pliku konfiguracyjnym unusedTables.properties)
     * (dane dotyczace baz baz danych zapisane w pliku postgresql.properties i mssql.properties)
     * (wszystkie pliki .properties zapisane w katalogu SuncodeDatabaseMigration znajduj�cym sie w katalogu domowym)
     * mssql/mssql-integratedSecurity clone server:port templateDBName targetDBName user password unusedTables 
     * mssql/mssql-integratedSecurity merge server:port templateDBName targetDBName user password unusedTables
     * 
     *  postgresql clone
     * 
     * MERGE EXAMPLES:
     * 
     * postgresql merge
     * postgresql merge schema-only
     * mssql merge
     * mssql merge schema-only
     * EXPORT
     * 
     * export-postgresql "C:\Users\Adam\Desktop\dvdrental.zip"  //nazwa taka sama jak nazwa bazy danych
     * export-mssql "C:\Users\Adam\Desktop\pubs_copy.zip"
     * 
     * IMPORT
     * 
     * import-postgresql "C:\Users\Adam\Desktop\skopiowana3000.zip"
     * import-mssql "C:\Users\Adam\Desktop\pubs_copy.zip"
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
	
	final static Logger logger = Logger.getLogger(Migration.class);
	
    public static void main( String[] args ) throws SQLException, ClassNotFoundException, IOException
    {
        boolean result = false;
        boolean integratedSecurity = false;
        List<String> tablesNames = new ArrayList<String>();
        List<String> unusedSequencesNames = new ArrayList<String>();
        List<String> unusedMergeSequencesNames = new ArrayList<String>();
        
        // arg[0] = postgres/mssql; arg[1] = clone/merge
        
        if(args.length == 1) {
        	if(args[0].equals("help")) {
        		Help help = new Help();
        		help.showMessage();
        		return;
        	} else {
        		logger.info("Unrecognized argument: " + args[0]);
        		return;
        	}
        }
        
        boolean enoughArguments  = checkArguments(args);
        if(!enoughArguments) {
    		logger.info("Program terminated: " + args[0]);
        	return;
        }
        
        String databaseType = args[0];
        String operationType = args[1];
        String mode = "";
        String dbAdress = ""; 
        String templateDatabaseName = "";
        String userName = "";
        String password = "";
        String targetUserName = "";
        String targetPassword = "";
        String pg_dumpPath = "";
        String psqlPath = "";
        String targetDatabaseName = "";
        String hostAndPort = "";
        String host = "";
        String port = "";
        String targetHost = "";
        String targetPort = "";
        String adminDatabaseName = "";
        String adminUserName = "";
        String adminPassword = "";
        String restoreDatabaseName = "";
        String secondDatabaseName = "";
        String secondUserName = "";
        String secondPassword = "";
        String propertiesFile = "";
        String targetHostAndPort = "";
        String targetDbAddress = "";
        boolean schemaOnly = false;
        boolean updateSequences = false;
        boolean createMergeSequences = false;
        
     	if(databaseType.equals("postgresql") || databaseType.equals("mssql")) {
     		if(args.length == 5) {
     			propertiesFile = args[4];
     		} else {
     			propertiesFile = args[3];
     		}
    		
    	} else {
    		propertiesFile = args[2];
    	}
        
        if(databaseType.equals("postgresql") || databaseType.equals("import-postgresql") || databaseType.equals("export-postgresql")) {
       
        	host = getPropertyFromFile(propertiesFile, "postgresql.host");// "//" + args[1];     //"localhost:5432";
        	port = getPropertyFromFile(propertiesFile, "postgresql.port");
        	targetHost = getPropertyFromFile(propertiesFile, "postgresql.targetHost");
        	targetPort = getPropertyFromFile(propertiesFile, "postgresql.targetPort");
        	
        	templateDatabaseName = getPropertyFromFile(propertiesFile, "postgresql.databaseName");
        	targetDatabaseName = getPropertyFromFile(propertiesFile, "postgresql.targetDatabaseName");
        	userName = getPropertyFromFile(propertiesFile, "postgresql.userName");
        	password = getPropertyFromFile(propertiesFile, "postgresql.password");
        	pg_dumpPath = getPropertyFromFile(propertiesFile, "postgresql.pg_dumpPath");
        	psqlPath = getPropertyFromFile(propertiesFile, "postgresql.psqlPath");
        	adminDatabaseName = getPropertyFromFile(propertiesFile, "postgresql.adminDatabaseName");
        	adminUserName = getPropertyFromFile(propertiesFile,  "postgresql.adminUserName");
        	adminPassword = getPropertyFromFile(propertiesFile, "postgresql.adminPassword");
        	restoreDatabaseName = getPropertyFromFile(propertiesFile, "postgresql.restoreDatabaseName");
        	secondDatabaseName = getPropertyFromFile(propertiesFile, "postgresql.secondDatabaseName");
        	secondUserName = getPropertyFromFile(propertiesFile, "postgresql.secondUserName");
        	secondPassword = getPropertyFromFile(propertiesFile, "postgresql.secondPassword");
        	updateSequences = getBooleanFromFile(propertiesFile, "postgresql.updateSequences");
        	createMergeSequences = getBooleanFromFile(propertiesFile, "postgresql.createMergeSequences");
        	hostAndPort = host + ":" + port;
        	dbAdress = "//" + host + ":" + port;
        	targetHostAndPort = targetHost + ":" + targetPort;
        	targetDbAddress = "//" + targetHost + ":" + targetPort;
        } else {
        	host = getPropertyFromFile(propertiesFile, "mssql.host");
        	port = getPropertyFromFile(propertiesFile, "mssql.port");
        	targetHost = getPropertyFromFile(propertiesFile, "mssql.targetHost");
        	targetPort = getPropertyFromFile(propertiesFile, "mssql.targetPort");
        	adminPassword = getPropertyFromFile(propertiesFile, "mssql.adminPassword");
        	adminUserName = getPropertyFromFile(propertiesFile, "mssql.adminUserName");
        	
        	
        	templateDatabaseName = getPropertyFromFile(propertiesFile, "mssql.databaseName");
        	targetDatabaseName = getPropertyFromFile(propertiesFile, "mssql.targetDatabaseName");
        	userName = getPropertyFromFile(propertiesFile, "mssql.userName");
        	password = getPropertyFromFile(propertiesFile, "mssql.password");
        	adminDatabaseName = getPropertyFromFile(propertiesFile, "mssql.adminDatabaseName");
        	integratedSecurity = getIntegratedSecurity(propertiesFile);
        	secondDatabaseName = getPropertyFromFile(propertiesFile, "mssql.secondDatabaseName");
        	secondUserName = getPropertyFromFile(propertiesFile, "mssql.secondUserName");
        	secondPassword = getPropertyFromFile(propertiesFile, "mssql.secondPassword");
        	hostAndPort = host + ":" + port;
        	dbAdress = "//" + host + ":" + port;
        }
        
        
        //-----------------------------------------------EXPORT--------------------------------------------------------------
        if(args[0].equals("export-postgresql")) {
        	if(args.length < 3) {
        		logger.info("Not enough arguments provided.");
        	} else {
        		String targetZipDirectoryPath = args[1];
        		logger.info("Exporting database to zip file...");
        		
        		dbAdress += "/" + templateDatabaseName;
        		
                Connection connection = getDatabaseConnection(dbAdress, userName, password, "postgresql", hostAndPort,
                                                              templateDatabaseName, integratedSecurity);
        		List<String> allTablesNames = getDatabaseTablesNames(connection);
        		List<String> unusedTablesNames = readUnusedTablesNamesFromProperties( propertiesFile );
        		unusedSequencesNames = readUnusedSequencesNamesFromProperties( propertiesFile );
        		
        		
                exportPostgresToSQLFileForSelectedTables(pg_dumpPath, host, 
                		port, userName, password, templateDatabaseName, System.getProperty("user.home") + File.separator + templateDatabaseName + "1" + ".backup",
                		allTablesNames, unusedTablesNames, unusedSequencesNames);
                addDatabaseToZipArchive(templateDatabaseName, targetZipDirectoryPath);
        	}
            
        } else if(args[0].equals("export-mssql")) {
        	logger.info("Exporting mssql database to zip file");	
        	/**
        	 * export-mssql integratedSecurity databaseName hostAndPort zipPath
        	 */
        	String zipPath = args[1];
        	//Connection connection = getDatabaseConnection(dbAdress, userName, password, databaseType, hostAndPort, templateDatabaseName, integratedSecurity);
        	Connection connection = getConnectionMsSQL(dbAdress, userName, password, hostAndPort, templateDatabaseName, integratedSecurity);
        	createMSSQLBackup(templateDatabaseName, connection);
        	addMssqlDatabaseToZipArchive(templateDatabaseName, zipPath);
        	logger.info("Zip file created in selected location");
        
        //-----------------------------------------------IMPORT--------------------------------------------------------------
        } else if(args[0].equals("import-postgresql")) {
        	if(args.length  < 3) {
        		logger.info("Not enough arguments provided.");
        	} else {
        		String fullZipPath = args[1];
        		logger.info("Importing postgreSQL database from zip file...");
            	//restorePostgresqlDatabase(psqlPath, host, port, adminUserName, adminPassword, adminDatabaseName,restoreDatabaseName, fullZipPath, adminDatabaseName);	
            	restorePostgresqlDatabase( psqlPath, host, port, adminUserName, adminPassword, templateDatabaseName, restoreDatabaseName, fullZipPath, adminDatabaseName );
        	}
        	/**
        	 * import-mssql databaseName adminDatabase userName password hostAndPort zipPath
        	 */
        } else if(args[0].equals("import-mssql")) {
        	
        	String zipPath = args[1];
        	logger.info("Importing database from zip file...");
        	restoreMssqlDatabaseFromZip(templateDatabaseName, adminDatabaseName, userName, password, hostAndPort, zipPath);
        } else {
        	
        	
        	//----------------------------------------------CLONE-----------------------------------------------------------
			if (operationType.equals("clone")) {
				logger.info("Performing database migration...");
				
				mode = args[2];
				
				//String dbAdressCopy = dbAdress + "/" + targetDatabaseName;
				String dbAdressCopy = targetDbAddress + "/" + targetDatabaseName;
				String adminDbAddress = targetDbAddress + "/" + adminDatabaseName;
				
				dbAdress += "/" + templateDatabaseName;
				

				Connection connection = getDatabaseConnection(dbAdress, userName, password, databaseType, hostAndPort,
						templateDatabaseName, integratedSecurity);
				
				tablesNames = readUnusedTablesNamesFromProperties(propertiesFile);
				unusedSequencesNames = readUnusedSequencesNamesFromProperties(propertiesFile);
				
				for(String table :  tablesNames) {
					logger.info("Loaded table: " + table);
				}
				
				List<String> allTablesNames = getDatabaseTablesNames(connection);
				
				Connection adminConnection;
				if(databaseType.equals("postgresql")) {
					adminConnection = getConnectionPostgreSQL(adminDbAddress, adminUserName, adminPassword);	
				} else {
					adminConnection = getConnectionMsSQL(adminDbAddress, adminUserName, adminPassword, targetHostAndPort, adminDatabaseName, integratedSecurity);
				}
				
				
				result = copySchema(connection, templateDatabaseName, targetDatabaseName, userName, password,
						hostAndPort, allTablesNames, tablesNames, integratedSecurity, pg_dumpPath, psqlPath, mode,
						adminConnection, targetHostAndPort, adminUserName, adminPassword, updateSequences,
						unusedSequencesNames);

				if (result) {
					logger.info("Database migrated successfully.");
				} else {
					logger.info("Database migration terminated.");
				}
				
				
				
				//------------------------------------MERGE------------------------------------------------------------------
			} else if(operationType.equals("merge")) {
				
				logger.info("Merging postgresql database...");
				String adminDbAddress = targetDbAddress + "/" + adminDatabaseName;
				
				
				mode = args[2];
				//Najpierw normalna kopia pierwotnej bazy do bazy docelowej
				String dbAdressCopy = targetDbAddress + "/" + targetDatabaseName;
				String secondDBAdress = dbAdress + "/" + secondDatabaseName;
				
				dbAdress += "/" + templateDatabaseName;
				
				Connection connection = getDatabaseConnection(dbAdress, userName, password, databaseType, hostAndPort,
						templateDatabaseName, integratedSecurity);
				
				tablesNames = readUnusedTablesNamesFromProperties(propertiesFile);
				
				for (int i = 0; i < tablesNames.size(); i++) {
					logger.info("Loaded unused table from properties file: " + tablesNames.get(i));
				}
				
				Connection adminConnection;
				if(databaseType.equals("postgresql")) {
					adminConnection = getConnectionPostgreSQL(adminDbAddress, adminUserName, adminPassword);	
				} else {
					adminConnection = getConnectionMsSQL(adminDbAddress, adminUserName, adminPassword, targetHostAndPort, adminDatabaseName, integratedSecurity);
				}
				List<String> allTablesNames = getDatabaseTablesNames(connection);
				
				System.out.println("All tables names size: " + allTablesNames.size());
				
                unusedSequencesNames = readUnusedSequencesNamesFromProperties(propertiesFile);
				
				result = copySchema(connection, templateDatabaseName, targetDatabaseName, userName, password,
						hostAndPort, allTablesNames, tablesNames, integratedSecurity, pg_dumpPath, psqlPath, mode,
					adminConnection, targetHostAndPort, adminUserName, adminPassword, updateSequences, 
					unusedSequencesNames);
				
				
				
				//Potem skopiowanie tabel z secondDatabaseName do stworzonej wczesniej bazy
				if(databaseType.equals("postgresql")) {
					List<String> mergeTables = readMergeTablesNamesFromProperties(propertiesFile);
					
					for(String table: mergeTables) {
						logger.info("Loaded merge table from properties file : " + table);
					}
					
					Connection secondConnection = getConnectionPostgreSQL(secondDBAdress, userName, password);
					Connection connectionCopy = getConnectionPostgreSQL(dbAdressCopy, adminUserName, adminPassword); ///////////// targetUserName i targetPasswords
					if(args.length == 5) {
						if(args[3].equals("schema-only")) {
							schemaOnly = true;
						}
					}
					
					unusedMergeSequencesNames = readUnusedMergeSequencesNamesFromProperties(propertiesFile);
					
					result = copyPostgresqlTablesToMergeDatabase(connectionCopy, secondConnection, mergeTables,
							hostAndPort, pg_dumpPath, userName, password, secondDatabaseName,
							psqlPath, targetDatabaseName, schemaOnly, createMergeSequences, unusedMergeSequencesNames, tablesNames);
					
					connectionCopy.close();
					secondConnection.close();
					removePostgresqlDatabase(connection, secondDatabaseName);
					renamePostgresqlDatabase(adminConnection, secondDatabaseName, targetDatabaseName);
					
				} else if(databaseType.equals("mssql")) {
					//MSSQL
					List<String> mergeTables = readMergeTablesNamesFromProperties(propertiesFile);
					Connection secondConnection = getConnectionMsSQL(secondDBAdress, secondUserName, secondPassword, hostAndPort, secondDatabaseName, integratedSecurity);
					Connection connectionCopy = getConnectionMsSQL(dbAdressCopy, userName, password, hostAndPort, targetDatabaseName, integratedSecurity);
					logger.info("Merging MsSQL database...");
					if(args.length == 3) {
						if(!args[2].equals("schema-only")) {
							logger.info("Unknown argument value: " + args[2]);
							return;
						}
						schemaOnly = true;
					}
					result = copyMsSQLTablesToMergeDatabase(connectionCopy, secondConnection, mergeTables,
								secondDatabaseName, targetDatabaseName, schemaOnly, mode);
					
					connectionCopy.close();
					secondConnection.close();
					removeMssqlDatabase(connection, secondDatabaseName);
					renameMssqlDatabase(connection, secondDatabaseName, targetDatabaseName);
					
				}
				
				if(result) {
					logger.info("Database merged successfully");
				} else {
					logger.info("Database merging terminated.");
				}
				
			} else {
				logger.info("Operation type not recognized.");
				return;
			}
        }                 
    }
    
    public static void removeMssqlDatabase(Connection connection, String databaseName) {
    	String query = "DROP DATABASE " + databaseName;
    	Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(query);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static void renameMssqlDatabase(Connection connection, String newDbName, String oldDbName) {
    	String query = "ALTER DATABASE " + oldDbName + " MODIFY NAME = " + newDbName;
    	Statement statement;
		try {
			statement = connection.createStatement();
	    	statement.execute(query);
	    	
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
    }
    
    public static void renamePostgresqlDatabase(Connection connection, String newDbName, String oldDbName) {
    	String query = "ALTER DATABASE " + oldDbName + " RENAME TO " + newDbName;
    	Statement statement;
		try {
			statement = connection.createStatement();
	    	statement.execute(query);
	    	
		} catch (SQLException e) {
			
			e.printStackTrace();
		}

    }
    
    public static void removePostgresqlDatabase(Connection connection, String databaseName) {
    	String query = "DROP DATABASE IF EXISTS " + databaseName;
    	Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(query);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
	public static boolean checkArguments(String[] arguments) {
		boolean result = false;
		if (arguments.length < 3) {
			logger.info("Not enough arguments provided");
			result = false;
		} else {
			if (arguments[1].equals("clone") || arguments[1].equals("merge")) {
				if (arguments.length < 4) {
					
					logger.info("Not enough arguments provided");
					logger.info("Required arguments: databaseType:postgresql/mssql operationType:clone/merge operationMode:safe/force propertiesPath");
					logger.info("More info: help");
					result = false;
				} else {
					if(!arguments[2].equals("safe") && !arguments[2].equals("force")) {
						logger.info("Unrecognized argument: " + arguments[2]);
						logger.info("Required arguments: databaseType:postgresql/mssql operationType:clone/merge operationMode:safe/force propertiesPath");
						logger.info("More info: help");
						result = checkIfFileExists(arguments[3]);
						if(!result) {
							logger.info("File does not exists: " + arguments[3]);
						}
					} else {
						result = true;
					}
				}
			} else if(arguments[0].equals("import-postgresql") || arguments[0].equals("import-mssql")) {
				//result = checkIfFileExists(arguments[1]);
				if(!checkIfFileExists(arguments[1])) {
                    logger.info("File does not exists: " + arguments[1]);
                    return false;
                }
				if(!checkIfFileExists(arguments[2])) {
					logger.info("File does not exists: " + arguments[2]);
					return false;
				}
				else {
				    return true;
				}
				
				//file.zip database.properties
			} else if(arguments[0].equals("export-postgresql") || arguments[0].equals("export-mssql")) {
				if(!checkIfFileExists(arguments[2])) {
					logger.info("File does not exists: " + arguments[2]);
					result = false;
				} else {
				    result = true;
				}
				
			} else {
				logger.info("Unknown argument: " + arguments[1]);
				logger.info("Required arguments: databaseType:postgresql/mssql operationType:clone/merge operationMode:safe/force");
				logger.info("More info: help");
				result = false;
			}
		}
		return result;
	}
	
	public static boolean checkIfFileExists(String path) {
		boolean result = false;
		
		String fileType = "";
		File f = new File(path);
		if(f.exists() && !f.isDirectory()) { 
			result = true;
		}
		
		return result;
	}
    
    public static boolean copyMsSQLTablesToMergeDatabase(Connection connectionCopy, Connection secondConnection,
    		List<String> mergeTables, String secondDatabaseName, String targetDatabaseName, boolean schemaOnly,
    		String mode) throws SQLException {
    	
    	boolean result = false;
    	
    	result = copyMsSQLTablesToTargetDatabase(secondConnection, connectionCopy, mergeTables, targetDatabaseName);
    	
		if (!schemaOnly) {
			try {
				result = copyMsSQLTablesContent(secondConnection, connectionCopy,targetDatabaseName, secondDatabaseName, mergeTables, "mssql", mode);
			} catch (SQLException e) {
				logger.debug(e);
				e.printStackTrace();
			}
		}

    	return result;
    }
    
    public static boolean copyPostgresqlTablesToMergeDatabase(Connection connectionCopy, 
    												Connection secondConnection, 
    												List<String> mergeTables,
    												String hostAndPort, String pg_dumpPath, String userName,
    												String password, String secondDatabaseName,
    												String psqlPath, String targetDatabaseName,
    												boolean schemaOnly, boolean createMergeSequences,
    												List<String> unusedMergeSequencesNames,
    												List<String> unusedTablesNames) {
    	boolean result = false;
    	
    	
    	//if(createMergeSequences) {
        	
    	    List<String> sequencesNames = getNamesOfSequencesForMergePostgresqlTables(secondConnection, mergeTables, unusedMergeSequencesNames);
        	//List<Long> startsWithNumbers = getStartsWithNumberForPostgresSequence(secondConnection, sequencesNames);
        	List<Long> startsWithNumbers = getStartsWithNumberForPostgresMergeSequences( secondConnection, sequencesNames, unusedMergeSequencesNames);
        	System.out.println( "Starts with numbers: " + startsWithNumbers);
        	result = createPostgresqlSequences(connectionCopy, sequencesNames, startsWithNumbers, schemaOnly, createMergeSequences, unusedMergeSequencesNames);	
    	//}
    	result = copyPostgresqlTablesToDatabase(hostAndPort, pg_dumpPath, userName, password, secondDatabaseName, 
    			mergeTables, psqlPath, targetDatabaseName, schemaOnly, unusedTablesNames);
    
    	return result;
    }
    
    
    public static boolean copyPostgresqlTablesToDatabase(String hostAndPort, String pg_dumpPath,
    		String userName, String password, String secondDatabaseName, List<String> mergeTables,
    		String psqlPath, String targetDatabaseName, boolean schemaOnly, List<String> unusedTablesNames) {
    	boolean result = false;
    	
    	
    	for(int i = 0 ; i < mergeTables.size(); i++) {
    	    boolean unusedTable = unusedTablesNames.contains( mergeTables.get(i));
        	copyPostgresqlTableSchemaToSqlFile(hostAndPort, pg_dumpPath, userName, password, 
        			secondDatabaseName, mergeTables.get(i), schemaOnly, unusedTable);    		
    	}
    	for(int i = 0; i < mergeTables.size(); i++) {
    		restorePostgresqlTableFromSqlFile(hostAndPort, psqlPath, targetDatabaseName,
    			userName, password, mergeTables.get(i));
    	}
    	result = true;
    	return result;
    }
    
    
    private static void executeSql(String path, Connection connection) throws SQLException {
    	  byte[] encoded;
		try {
			encoded = Files.readAllBytes(Paths.get(path));
			String query =  new String(encoded, "UTF8");
			
			Statement statement = connection.createStatement();
			statement.execute(query);
			
		} catch (IOException e) {
			logger.debug(e);
			e.printStackTrace();
		}
    	  
    	/*final class SqlExecuter extends SQLExec {
            public SqlExecuter() {
                Project project = new Project();
                project.init();
                setProject(project);
                setTaskType("sql");
                setTaskName("sql");
            }
        }

        SqlExecuter executer = new SqlExecuter();
        executer.setSrc(new File(sqlFilePath));
        executer.setDriver("org.postgresql.Driver");
        executer.setPassword("pguser");
        executer.setUserid("pguser");
        executer.setEncoding("UTF8");
        executer.setUrl("jdbc:postgresql://localhost/plusworkflowdev_copy");
        executer.execute(); */
    }
    
    public static void restorePostgresqlTableFromSqlFile(String hostAndPort, String psqlPath, String targetDatabaseName,
    		String userName, String password, String tableName) {
    	//psql -U postgres -d testowa -1 -f "C:/Users/Adam/Desktop/table.sql
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
        baseCmds.add(System.getProperty("user.home") + "/" + tableName +".sql");
   
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
            	logger.info(line);
                //System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();
            removeFile(System.getProperty("user.home") + "/" + tableName +".sql");

         } catch (IOException e) {
            e.printStackTrace();
            removeFile(System.getProperty("user.home") + "/" + tableName +".sql");
         } catch (InterruptedException ie) {
            ie.printStackTrace();
            removeFile(System.getProperty("user.home") + "/" + tableName +".sql");
         }
    	
    }
    
    
    public static void copyPostgresqlTableSchemaToSqlFile(String hostAndPort, String pg_dumpPath,
    		String userName, String password, String templateDatabaseName, String tableName, boolean schemaOnly,
    		boolean unusedTable) {
    	
    	//pg_dump -U postgres -t language dvdrental > C:/Users/Adam/Desktop/table.sql
    	String host = getHostFromAddress(hostAndPort);
    	String port = getPortFromAddress(hostAndPort);
    	
    	System.out.println( hostAndPort + " " + templateDatabaseName + " table: " + tableName); 
    	
        final List<String> baseCmds = new ArrayList<String>();
        baseCmds.add(pg_dumpPath);
        baseCmds.add("-h");
        baseCmds.add(host);
        baseCmds.add("-p");
        baseCmds.add(port);
        baseCmds.add("-U");
        baseCmds.add(userName);
        if(schemaOnly || unusedTable) {
            System.out.println( "Copying only schema for table: " + tableName);
        	baseCmds.add("-s");
        }
        baseCmds.add("-t");
        baseCmds.add(tableName);
        baseCmds.add("-v");
        baseCmds.add("-f");
        baseCmds.add(System.getProperty("user.home") + "/" + tableName +".sql");
        baseCmds.add(templateDatabaseName);
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
            	logger.info(line);
                //System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();

         } catch (IOException e) {
        	logger.debug(e);
            e.printStackTrace();
         } catch (InterruptedException ie) {
        	 logger.debug(ie);
            ie.printStackTrace();
         }
    }
    
    
    public static boolean createPostgresqlSequences(Connection connection, List<String> sequencesNames,
    												List<Long> startsWithNumbers, boolean schemaOnly,
    												boolean createMergeSequences, List<String> unusedMergeSequencesNames) {
    	boolean result = false;
    	String query = "";
    	try {
			Statement statement = connection.createStatement();
			for(int i = 0; i < sequencesNames.size(); i++) {
				
			    //System.out.println( sequencesNames.size() );
			    //System.out.println( "Unused merge sequences names size: " + unusedMergeSequencesNames.size() );
			    //System.out.println(unusedMergeSequencesNames.contains(sequencesNames.get(i)));
			    
				if(schemaOnly || !createMergeSequences || unusedMergeSequencesNames.contains(sequencesNames.get(i))) {
					query = "CREATE SEQUENCE " + sequencesNames.get(i) + " START 1";
					System.out.println( query );
				} else {
					query = "CREATE SEQUENCE " + sequencesNames.get(i) + " START " +
							String.valueOf(startsWithNumbers.get(i) + 1);
					System.out.println( query );
				}
				statement.execute(query);
			}
			result = true;
			
		} catch (SQLException e) {
			logger.debug(e);
			e.printStackTrace();
		}

    	return result;
    }
    
    public static List<Long> getStartsWithNumberForPostgresSequence(Connection connection, List<String> sequencesNames) {
    	List<Long> startsWithNumbers = new ArrayList<Long>();
    	
    	try {
			Statement statement = connection.createStatement();
			
			//System.out.println(sequencesNames.size());
	    	for(int i = 0; i < sequencesNames.size(); i++) {
	    		
	    		String query = "SELECT last_value FROM " + sequencesNames.get(i) ;
	    		ResultSet rs = statement.executeQuery(query);
	    		
	    		while(rs.next()) {
	    			Long number = rs.getLong(1);
	    			startsWithNumbers.add(number);
	    			//System.out.println(rs.getLong(1));
	    		}
	    	}
			
		} catch (SQLException e) {
			logger.debug(e);
			e.printStackTrace();
		}
	
    	return startsWithNumbers;
    	
    }
    
    public static List<Long> getStartsWithNumberForPostgresMergeSequences(Connection connection, List<String> sequencesNames,
                                                                         List<String> unusedSequencesNames) {
        List<Long> startsWithNumbers = new ArrayList<Long>();
        
        /*List<String> removedSequencesNames = new ArrayList<String>();
        removedSequencesNames = sequencesNames;
        removedSequencesNames = removeElementsFromList( removedSequencesNames, unusedSequencesNames ); */
        
        try {
            Statement statement = connection.createStatement();
            
            //System.out.println(sequencesNames.size());
            for(int i = 0; i < sequencesNames.size(); i++) {
                
                if(unusedSequencesNames.contains(sequencesNames.get( i ))) {
                    startsWithNumbers.add((long) 1);
                } else {
                    String query = "SELECT last_value FROM " + sequencesNames.get(i) ;
                    ResultSet rs = statement.executeQuery(query);
                    
                    while(rs.next()) {
                        Long number = rs.getLong(1);
                        startsWithNumbers.add(number);
                    }
                }

            }
            
        } catch (SQLException e) {
            logger.debug(e);
            e.printStackTrace();
        }
    
        return startsWithNumbers;
        
    }
    
    
    public static List<String> getNamesOfSequencesForMergePostgresqlTables(Connection secondConnection, List<String> mergeTables,
                                                                           List<String> unusedMergeSequencesNames) {
        List<String> sequencesNames = new ArrayList<String>();
        List<String> finalSequencesNames  = new ArrayList<String>();
        try {
            Statement statement = secondConnection.createStatement();
            String query = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';";
            
            ResultSet rs = statement.executeQuery(query);
            while(rs.next()) {
                //System.out.println(rs.getString(1));
                sequencesNames.add(rs.getString(1));
            }
            
        } catch (SQLException e) {
            logger.debug(e);
            e.printStackTrace();
        }
        
        for(String unused: unusedMergeSequencesNames) {
            System.out.println( "Unused merge sequence:----------------- " + unused );
        }
        //sequencesNames = removeElementsFromList(sequencesNames, unusedMergeSequencesNames);
        
        //usuniecie sekwencji, które nie zawieraja nazwy mergowanych tabeli 
        sequencesNames = removeSequencesNamesWithoutMergeTableName(sequencesNames, mergeTables);
        
        System.out.println( "Final sequences names size: "  + sequencesNames.size());

        return sequencesNames;
    }
    
    public static boolean checkIfSequenceBelongsToMergeTables(String sequenceName, List<String> mergeTablesNames) {
        for(String mergeTableName: mergeTablesNames) {
            if(sequenceName.contains( mergeTableName )) {
                return true;
            }
        }
        return false;
    }
    
    public static List<String> removeSequencesNamesWithoutMergeTableName( List<String> sequencesNames,
                                                                          List<String> mergeTablesNames )
    {
        List<String> finalSequencesNames = new ArrayList<String>();

        for (int i = 0; i < sequencesNames.size(); i++ )
        {
            boolean result = checkIfSequenceBelongsToMergeTables( sequencesNames.get( i ), mergeTablesNames );
            if ( result )
            {
                finalSequencesNames.add( sequencesNames.get( i ) );
            }
        }
        return finalSequencesNames;
    }
    
    public static List<String> getNamesOfSequencesForPostgresqlTables(Connection connection, List<String> tablesToCopy,
                                                                      List<String> unusedSequencesNames) {
    	List<String> sequencesNames = new ArrayList<String>();
    	List<String> finalSequencesNames  = new ArrayList<String>();
    	try {
			Statement statement = connection.createStatement();
			String query = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'S';";
			
			ResultSet rs = statement.executeQuery(query);
			while(rs.next()) {
				//System.out.println(rs.getString(1));
				sequencesNames.add(rs.getString(1));
			}
			
		} catch (SQLException e) {
			logger.debug(e);
			e.printStackTrace();
		}
    	
    	sequencesNames = removeElementsFromList(sequencesNames, unusedSequencesNames);
    	
    	System.out.println( "Final sequences names size: "  + sequencesNames.size());
    	/*for(int i = 0; i < sequencesNames.size(); i++) {
    		for(int j = 0; j < tablesToCopy.size(); j++) {
        		if(sequencesNames.get(i).contains(tablesToCopy.get(j))) {
        			finalSequencesNames.add(sequencesNames.get(i));
        		}	
    		}
    	}*/
    	//trzeba usuwac unused merge sequences
    	return sequencesNames;
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
        	logger.debug(e);
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
    		List<String> unusedTablesNames, boolean integratedSecurity, String pg_dumpPath, String psqlPath,
    		String mode, Connection adminConnection, String targetHostAndPort,
    		String adminUserName, String adminPassword,
    		boolean updateSequences, List<String> unusedSequencesNames) throws SQLException {
    	
        boolean result = false;
        DatabaseMetaData metaData = connection.getMetaData();
        String databaseType = metaData.getDatabaseProductName();
        
        if(databaseType.equals("Microsoft SQL Server")) {
        	String dbAddress = "//" + targetHostAndPort;
        	List<String> tablesToCopy = removeElementsFromList(allTablesNames, unusedTablesNames);
        	createDatabaseMigrationDirectory();
        	
        	createDatabaseMsSQL(adminConnection, targetDatabaseName);
        	
           // if(mode.equals("force")) {
            	//nie kopiuje tabel, kt�re wyrzucaj� fk exception podczas insertowania
            	//tablesToCopy = collectTablesWithoutFKException(tablesToCopy, connection, databaseType, targetDatabaseName);
            //}
        	
        	Connection connectionCopy = getConnectionMsSQL(dbAddress, adminUserName, adminPassword, targetHostAndPort, targetDatabaseName, integratedSecurity);
        	copyMsSQLTablesSchemaToTargetDatabase(connection, connectionCopy);
        	copyForeignKeysMssql(connection, connectionCopy);

        	result = copyMsSQLTablesContent(connection,connectionCopy,targetDatabaseName, templateDatabaseName, tablesToCopy, databaseType, mode);
        	connectionCopy.close();
        	
        } else if(databaseType.equals("PostgreSQL")) {
        	
        	String dbAddress = "//" + hostAndPort + "/" + targetDatabaseName;
        	String targetDbAddress = "//" + targetHostAndPort + "/" + targetDatabaseName;
        	
        	List<String> tablesToCopy = removeElementsFromList(allTablesNames, unusedTablesNames);
        	
        	//kopiowany jest schemat (sekwencje przy kopiowaniu s� zerowane)
            result = copyPostgreSQLSchemaToSQLFile(templateDatabaseName, pg_dumpPath, hostAndPort, userName, password);
            
         
        	createDatabasePostgresqlWithConnection(adminConnection, targetDatabaseName);
        	
        	//setUsernameAndPassword(adminConnection, targetDatabaseName, userName, password);
        	
            result = restoreDatabaseSchemaFromSQLFile(targetDatabaseName, targetHostAndPort, adminUserName, adminPassword, psqlPath);
            
            Connection connectionCopy = getConnectionPostgreSQL(targetDbAddress, adminUserName, adminPassword);
                    	
            if(mode.equals("force")) {
            	//nie kopiuje tabel, kt�re wyrzucaj� fk exception podczas insertowania
            	tablesToCopy = collectTablesWithoutFKException(tablesToCopy, connection, databaseType, templateDatabaseName);
            }
            
            //System.out.println(tablesToCopy.size());
            //Ustawianie 1 we wszystkich sekwencjach
            List<String> allSequencesNames = getNamesOfSequencesForPostgresqlTables( connectionCopy, tablesToCopy, new ArrayList<String>());
            List<Long> resetSequencesValues = new ArrayList<Long>();
            for(String seqName: allSequencesNames) {
                resetSequencesValues.add( (long) 1 );
            }
            updateStartWithPostgresqlSequences(connectionCopy, allSequencesNames, resetSequencesValues); 
            
            
            
        	List<String> sequencesNames = getNamesOfSequencesForPostgresqlTables(connection, tablesToCopy, unusedSequencesNames);
        	List<Long> startsWithNumbers = getStartsWithNumberForPostgresSequence(connection, sequencesNames);
            
        	//update na sekwencjach i ustawienie STARTS WITH z poprzedniej bazy
        	//result = removeSequencesFromPostgresqlDatabase(connectionCopy, sequencesNames);
        	
        	for(int i = 0; i < sequencesNames.size(); i++) {
        	    System.out.println( "Sequence: " + sequencesNames.get( i ) + "  , value: " + startsWithNumbers.get( i ) );
        	}
        	
        	
        	if(updateSequences) {
        		result = updateStartWithPostgresqlSequences(connectionCopy, sequencesNames, startsWithNumbers);        		
        	}
            
        	//Connection connectionCopy = getConnectionPostgreSQL(dbAddress, userName, password);

            result = copyPostgresqlTablesContent(connection, connectionCopy, templateDatabaseName, targetDatabaseName, tablesToCopy, databaseType, mode);
            connectionCopy.close();
        }
        return result;
        
    }
    
    public static void setUsernameAndPassword(Connection adminConnection, String targetDatabaseName, String userName, String password) {
        String query = "CREATE USER " + userName +  " WITH PASSWORD '" + password + "';";
        query += "GRANT ALL PRIVILEGES ON DATABASE " + targetDatabaseName + " to " + userName;
        
        Statement statement;
        try
        {
            statement = adminConnection.createStatement();
            statement.execute( query );
        }
        catch ( SQLException e )
        {
            logger.error( e );
        }
        
    }
    
    public static List<String> getSchemasNamesForMSSQLTables(Connection connection, List<String> allTablesNames) throws SQLException {
        
    	List<String> schemasNames = new ArrayList<String>();
    	List<String> allSchemasNames = new ArrayList<String>();
        DatabaseMetaData md = connection.getMetaData();
        ResultSet rs;

        rs = md.getSchemas("", null);

        while (rs.next()) {
       
            schemasNames.add(rs.getString(1));
        }
        
        rs = md.getSchemas();
        while (rs.next()) {
            
            allSchemasNames.add(rs.getString(1));
        }
        List<String> finalSchemasNames = removeElementsFromList(allSchemasNames, schemasNames);
        
        return finalSchemasNames;
    }
    
    public static boolean updateStartWithPostgresqlSequences(Connection connection, List<String> sequencesNames,
    														List<Long> startsWithNumbers) {
    	boolean result = false;
    	for(int i = 0; i < sequencesNames.size(); i++) {
    		try {
				Statement statement = connection.createStatement();
				String query = "ALTER SEQUENCE " + sequencesNames.get(i) + " RESTART WITH " + (startsWithNumbers.get(i) + 1);
				logger.info(query);
				//System.out.println(query);
				statement.execute(query);
			} catch (SQLException e) {
				e.printStackTrace();
			}
    		result = true;
    	}
    	return result;
    }
    
    public static boolean removeSequencesFromPostgresqlDatabase(Connection connection, List<String> sequencesNames) {
    	boolean result = false;
    	for(int i = 0; i < sequencesNames.size(); i++) {
    		try {
				Statement statement = connection.createStatement();
				String query = "DROP SEQUENCE IF EXISTS " + sequencesNames.get(i);
				logger.info(query);
				statement.execute(query);
			} catch (SQLException e) {
				e.printStackTrace();
			}
    		result = true;
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
        
        logger.info("Restoring database...");
        final ProcessBuilder pb = new ProcessBuilder(baseCmds);
        //psql -d database_name -h localhost -U postgres < path/db.sql
        
        // Set the password
        final Map<String, String> env = pb.environment();
        env.put("PGPASSWORD", password);

        try {
            final Process process = pb.start();
            final BufferedReader r = new BufferedReader(
                      new InputStreamReader(process.getInputStream()));
           // String line = r.readLine();
            
            String allLines = "";
            String line;
            int i = 0;
            while ((line = r.readLine()) != null) {
                allLines += line;
            }

            while (line != null) {
            	logger.info(line);
                //System.err.println(line);
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
    	
        logger.info("Restoring finished");
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
            	logger.info(line);
                //System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();

         } catch (IOException e) {
            //e.printStackTrace();
            logger.error(e.getMessage());
         } catch (InterruptedException ie) {
        	logger.error(ie);
            //ie.printStackTrace();
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
    	String userHome = System.getProperty("user.home");
    	query += "BACKUP DATABASE " + templateDatabaseName + " TO DISK = '" + userHome + File.separator + "DatabaseMigration" + File.separator + templateDatabaseName + ".bak'  \n";
    	logger.info(query);
    	//System.out.println(query);
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
        	logger.error("Error while executing");
            logger.error(e.getMessage());
            //e.printStackTrace();
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
            logger.info("Database restored successfully.");
        }
        catch ( SQLException e )
        {
            logger.error("Error while restoring database.");
            removeFile("C:/DatabaseMigration/" + databaseName + ".bak");
            logger.error(e.getMessage());
            //e.printStackTrace();
            return;
        }
    }
    
    public static void createMssqlDatabase(Connection connection, String databaseName) {
    	String query = "CREATE DATABASE " + databaseName +";";
    	//System.out.println(query);
    	logger.info(query);
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query); 
        }
        catch ( SQLException e )
        {
        	logger.error("Error while creating database.");
        	logger.error(e.getMessage());
            //e.printStackTrace();
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
        	logger.error("Error while executing.");
            //System.out.println("Error while executing.");
        	logger.error(e.getMessage());
            //e.printStackTrace();
            return;
        }
    	
    }
    
    public static void deleteDatabaseMigrationFile(String templateDatabaseName) {

    	try{		
    		File file = new File("c:/DatabaseMigration/" + templateDatabaseName + ".bak");
        	file.delete();
    	}catch(Exception e){
    		logger.error(e.getMessage());
    		//e.printStackTrace();
    	}
    }
    
    
    public static void removeFile(String filePath) {
    	try{		
    		File file = new File(filePath);
        	file.delete();
    	}catch(Exception e){
    		logger.error(e.getMessage());
    		//e.printStackTrace();
    	}
    }
    
	public static void createDatabaseMigrationDirectory() {
		File theDir = new File(System.getProperty("user.home") + File.separator + "DatabaseMigration");

		if (!theDir.exists()) {
			boolean result = false;

			try {
				theDir.mkdir();
				result = true;
			} catch (SecurityException se) {
				logger.error(se);
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
        
        //rs.next();
        //System.out.println(rs.getString("TABLE_NAME"));
        while (rs.next()) {
            if(rs.getString("TABLE_TYPE") != null) {
                if(rs.getString("TABLE_TYPE").equals("TABLE")) {
                	if(databaseType.equals("PostgreSQL")) {
                        tablesNames.add(rs.getString("TABLE_NAME"));
                	} else {
                        //tablesNames.add("[" + rs.getString("TABLE_CAT") + "]." +  "[" + rs.getString("TABLE_NAME") + "]");
                        tablesNames.add(rs.getString("TABLE_NAME"));
                	}
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
        	logger.error("Wrong database type name");
        }
        return connection;
    }
    
    private static Connection getConnectionMsSQL(String dbAdress, String userName, String password, String hostAndPort, String databaseName, boolean integratedSecurity) {
        try {
            //Class.forName("org.postgresql.Driver");
        	Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
        	logger.error(e.getMessage());
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
        	//System.out.println(url);
        	logger.info(url);
            //"jdbc:microsoft:sqlserver://HOST:1433;DatabaseName=DATABASE";
            // root password
        } catch (SQLException e) {
            logger.error("Connection to: " + dbAdress + " failed.");
            logger.error(e.getMessage());
            //e.printStackTrace();
        }
        return connection;
    }
    
    private static Connection getConnectionPostgreSQL(String dbAdress, String userName, String password) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
        	logger.error(e.getMessage());
            //e.printStackTrace();
        }

        Connection connection = null;

        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql:" + dbAdress, userName, password);
            logger.info(dbAdress);
        } catch (SQLException e) {
            logger.error("Connection to: " + dbAdress + " failed.");
            logger.error(e.getMessage());
        }
        return connection;
    }
    
    
    public static void exportPostgresToSQLFileForSelectedTables( String pg_dumpPath, String host, String port,
                                                String userName, String password, String databaseName,
                                                String targetDirectoryPath, List<String> tablesNames,
                                                List<String> unusedTablesNames, List<String> unusedSequencesNames) throws IOException
    {
        
        final List<String> baseCmds = new ArrayList<String>();
        baseCmds.add( pg_dumpPath );
        baseCmds.add( "-h" );
        baseCmds.add( host );
        baseCmds.add( "-p" );
        baseCmds.add( port );
        baseCmds.add( "-U" );
        baseCmds.add( userName );

        for(String unusedTable: unusedTablesNames) {
            baseCmds.add( "-T" );
            baseCmds.add(unusedTable);
        }
        for(String unusedSequence: unusedSequencesNames) {
            baseCmds.add( "-T" );
            baseCmds.add(unusedSequence);
        }
        baseCmds.add( "-f" );
        baseCmds.add( targetDirectoryPath );
        baseCmds.add( databaseName );
        final ProcessBuilder pb = new ProcessBuilder( baseCmds );
                                
        // Set the password
        final Map<String, String> env = pb.environment();
        env.put( "PGPASSWORD", password );

        try
        {
            final Process process = pb.start();

            final BufferedReader r = new BufferedReader(new InputStreamReader( process.getErrorStream() ) );
            String line = r.readLine();
            while ( line != null )
            {
                logger.info( line );
                // System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();

        }
        catch ( IOException e ) {
            logger.error( e.getMessage() );
        }
        catch ( InterruptedException ie ) {
            logger.error( ie );
        }
        
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        final List<String> baseCmdsUnusedTables = new ArrayList<String>();
        baseCmdsUnusedTables.add(pg_dumpPath);
        baseCmdsUnusedTables.add("-h");
        baseCmdsUnusedTables.add(host);
        baseCmdsUnusedTables.add("-p");
        baseCmdsUnusedTables.add(port);
        baseCmdsUnusedTables.add("-U");
        baseCmdsUnusedTables.add(userName);
        baseCmdsUnusedTables.add("-s");
        for(String unusedTable: unusedTablesNames) {
            baseCmdsUnusedTables.add("-t");
            baseCmdsUnusedTables.add(unusedTable);   
        }
        baseCmdsUnusedTables.add("-v");
        baseCmdsUnusedTables.add("-f");
        baseCmdsUnusedTables.add(System.getProperty("user.home") + File.separator + databaseName + "2" + ".backup");
        baseCmdsUnusedTables.add(databaseName);
        final ProcessBuilder pb1 = new ProcessBuilder(baseCmdsUnusedTables);
        
        // Set the password
        final Map<String, String> env1 = pb.environment();
        env1.put("PGPASSWORD", password);

        try {
            final Process process = pb1.start();

            final BufferedReader r = new BufferedReader(
                      new InputStreamReader(process.getErrorStream()));
            String line = r.readLine();
            while (line != null) {
                logger.info(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();

         } catch (IOException e) {
            logger.debug(e);
            e.printStackTrace();
         } catch (InterruptedException ie) {
             logger.debug(ie);
            ie.printStackTrace();
         }
        
        String secondFilePath = System.getProperty("user.home") + File.separator + databaseName + "2" + ".backup";
        concatenateFiles(targetDirectoryPath, secondFilePath, databaseName);
        
        
    }    
    
    public static void concatenateFiles(String firstFile, String secondFile, String databaseName) throws IOException {
        File outFile = new File(System.getProperty("user.home") + File.separator + databaseName  + ".backup");
        OutputStream out = new FileOutputStream(outFile);
        byte[] buf = new byte[1024];
        String files[] = new String[2];
        files[0] = firstFile;
        files[1] = secondFile;
        for (String file : files) {
            InputStream in = new FileInputStream(file);
            int b = 0;
            while ( (b = in.read(buf)) >= 0) {
                out.write(buf, 0, b);
                out.flush();
            }
        }
        out.close();
        removeFile(firstFile);
        removeFile(secondFile);
    }
    
    
    /**
     *Exportuje schemat postgresql 
     * 
     **/
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
        baseCmds.add("-s");
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
            	logger.info(line);
                //System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();

         } catch (IOException e) {
        	 logger.error(e.getMessage());
            //e.printStackTrace();
         } catch (InterruptedException ie) {
        	 logger.error(ie);
            //ie.printStackTrace();
         }
    }
    
    public static void addDatabaseToZipArchive(String templateDatabaseName, String targetZipDirectoryPath) throws IOException {
    	  	
    	FileOutputStream fos;
		try {
			String filePath = System.getProperty("user.home") + File.separator + templateDatabaseName + ".backup";
			File file = new File(System.getProperty("user.home") + File.separator + templateDatabaseName + ".backup");
			fos = new FileOutputStream(targetZipDirectoryPath);
			ZipOutputStream zos = new ZipOutputStream(fos);
			addToZipFile(filePath, zos);
			zos.close();
			fos.close();
			removeFile(filePath);
			logger.info("Zip file created successfully.");
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
			logger.error("Exception while exporting.");
		}
    }
    
    public static void addMssqlDatabaseToZipArchive(String databaseName, String targetZipDirectoryPath) throws IOException {
	  	
    	FileOutputStream fos;
		try {
			String userHome = System.getProperty("user.home");
			String filePath = userHome + File.separator + "DatabaseMigration" + File.separator + databaseName + ".bak";
			File file = new File(userHome + File.separator + "DatabaseMigration" + File.separator + databaseName + ".bak");
			fos = new FileOutputStream(targetZipDirectoryPath);
			ZipOutputStream zos = new ZipOutputStream(fos);
			addToZipFile(filePath, zos);
			zos.close();
			fos.close();
			removeFile(filePath);
			logger.info("Zip file created successfully.");
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
			logger.error("Exception while importing.");
		}
    }
    
    public static void addToZipFile(String fileName, ZipOutputStream zos) throws FileNotFoundException, IOException {

		logger.info("Writing '" + fileName + "' to zip file");

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
    		String userName, String password, String databaseName, String restoreDatabaseName, String fullZipPath, String adminDatabaseName) {
    	
    	String dbAddress = "//" + host + ":" + port + "/" + adminDatabaseName;
    	createDatabasePostgresql(dbAddress, userName, password, restoreDatabaseName);
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
        baseCmds.add(restoreDatabaseName);
        baseCmds.add("-f");
        baseCmds.add(sqlPath); 
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
            	logger.info(line);
                //System.err.println(line);
                line = r.readLine();
            }
            r.close();

            final int dcertExitCode = process.waitFor();
            logger.info("Database imported successfully.");
         } catch (IOException e) {
        	logger.error(e.getMessage());
            //e.printStackTrace();
         } catch (InterruptedException ie) {
        	logger.error(ie); 
            //ie.printStackTrace();
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
              
              logger.info("file unzip : "+ newFile.getAbsoluteFile());
              //System.out.println("file unzip : "+ newFile.getAbsoluteFile());
                   
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
       	
       	logger.info("Zip loaded");
       		
       }catch(IOException ex){
          ex.printStackTrace(); 
       }
      } 
    
    public static void createDatabasePostgresql(String dbAdress, String userName, String password,
    		String databaseName) {
    	Connection connection = getConnectionPostgreSQL(dbAdress, userName, password);
    	String query = "CREATE DATABASE " + databaseName +";";
    	System.out.println( dbAdress);
    	logger.info(query);
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
        	logger.error("Error while creating database.");
            logger.error(e.getMessage());
            //e.printStackTrace();
            return;
        }
    }
    
    public static void createDatabaseMsSQL(Connection adminConnection, String targetDatabaseName) {
    	
    	//Connection connection = getConnectionMsSQL(dbAdress, userName, password, hostAndPort, templateDatabaseName, integratedSecurity);
    	String query = "if db_id('" + targetDatabaseName +"') is null CREATE DATABASE " + targetDatabaseName +";";
    	
    	logger.info(query);
    	Statement statement;
        try
        {
            statement = adminConnection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
        	logger.error("Error while creating database.");
        	logger.error(e.getMessage());
            //e.printStackTrace();
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
    	//query += "	print @SQL \n";
    	query += "  SELECT @SQL \n";
    	//query += " EXEC sys.sp_executesql @SQL";
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
    	query += "            when 'geography' then ' ' ";
    	query += "            when 'hierarchyid' then ' '";
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
    
    public static String getCopyFKConstrainstQueries(String tableName) {
    	String query = "";
    	query += "select  'ALTER TABLE '+object_name(a.parent_object_id)+ \n";
    	query += "    	' ADD CONSTRAINT '+ a.name + \n";
    	query += "' FOREIGN KEY (' + c.name + ') REFERENCES ' + \n";
    	query += "object_name(b.referenced_object_id) + \n";
    	query += " ' (' + d.name + ')' \n";
    	query += " from    sys.foreign_keys a \n";
    	query += "    join sys.foreign_key_columns b \n";
    	query += "              on a.object_id=b.constraint_object_id \n";
    	query += "    join sys.columns c \n";
    	query += "              on b.parent_column_id = c.column_id \n";
    	query += "         and a.parent_object_id=c.object_id \n";
    	query += "    join sys.columns d \n";
    	query += "              on b.referenced_column_id = d.column_id \n";
    	query += "        and a.referenced_object_id = d.object_id \n";
    	query += " where   object_name(b.referenced_object_id) in \n";
    	query += " ('" + tableName + "') \n";
    	query += " order by c.name \n";
    	return query;
    }
    
    public static void copyForeignKeysMssql(Connection connection, Connection connectionCopy) {
    	
    	try {
			Statement statement = connection.createStatement();
			Statement statementCopy = connectionCopy.createStatement();
			
			List<String> tablesNames = getDatabaseTablesNames(connection);
			
			for(int i = 0; i <tablesNames.size(); i++) {
				//zwraca funkcj�, kt�ra po wywo�aniu zwraca odpowiednie zapytanie
				String functionQuery = getCopyFKConstrainstQueries(tablesNames.get(i));
				ResultSet rs = statement.executeQuery(functionQuery);
				//String createConstraint = "";
				
				List<String> createConstraints = new ArrayList<String>();
				
				while(rs.next()) {
					createConstraints.add(rs.getString(1));
					//System.out.println(rs.getString(1));
				}
				for(int j = 0; j < createConstraints.size(); j++) {
					statementCopy.execute(createConstraints.get(j));					
				}
			}
			
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		}
    	
    }
    
    public static void copyMsSQLTablesSchemaToTargetDatabase(Connection connection, Connection connectionCopy) {
    	try {
				
				// Nie kopiuje kluczy obcych
				String query = getMsSQLCreateTablesQuery();
				
				Statement statement = connection.createStatement();
				Statement statementCopy = connectionCopy.createStatement();

				ResultSet rs = statement.executeQuery(query);
				while(rs.next()) {
					String createTableQuery = rs.getString(1);
					logger.info(createTableQuery);
				   
					statementCopy.executeUpdate(createTableQuery);
				}

		} catch (SQLException e) {
			logger.error(e.getMessage());
		}
    }
    
	public static boolean copyMsSQLTablesToTargetDatabase(Connection connection, Connection connectionCopy, 
			List<String> mergeTablesNames, String targetDatabaseName) throws SQLException {
		boolean result = false;
		try {
			//System.out.println("Merge tables names size: " + mergeTablesNames.size());
			Statement statementCopy = connectionCopy.createStatement();
			Statement statement = connection.createStatement();
			
			for(int i = 0; i < mergeTablesNames.size(); i++) {
				//zwraca funkcj�, kt�ra po wywo�aniu zwraca odpowiednie zapytanie
				String functionQuery = getMsSQLCreateTableQuery(mergeTablesNames.get(i));
				ResultSet rs = statement.executeQuery(functionQuery);
				String createTable = "";
				
				while(rs.next()) {
					createTable = rs.getString(1);
					logger.info(createTable);
				}
				statementCopy.execute(createTable);
				// dropDatabse(connectionCopy, targetDatabaseName);

				//statement.execute(query);  */
			}
			result = true;
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
			connectionCopy.close();
			dropDatabse(connection, targetDatabaseName);
		}
		return result;
	}
	
	
	public static void dropDatabse(Connection connection, String databaseName) {
		Statement statement;
		try {
			statement = connection.createStatement();
			String query = "DROP DATABASE " + databaseName;
			statement.execute(query);
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		}

	}
    
    public static boolean copyMsSQLTablesContent(Connection connection, Connection connectionCopy, String targetDatabaseName,String templateDatabaseName,
    	List<String> tablesToCopy, String databaseType, String mode) throws SQLException {
    	
    	List<String> orderedTablesToCopy = new ArrayList<String>();
    	
    	/*for(int i = 0; i < tablesToCopy.size(); i++) {
    		System.out.println(tablesToCopy.get(i));
    	}*/
    	
    	//System.out.println("Ordered tables list: ");
    	logger.info("Ordered tables list: ");
    	if(mode.equals("safe")) {
    		orderedTablesToCopy = collectTablesList(tablesToCopy, connectionCopy, databaseType, templateDatabaseName);
    	} else {
    		orderedTablesToCopy = collectTablesWithoutFKException(tablesToCopy, connectionCopy, databaseType, templateDatabaseName);
    	}
    	
    	
    	for(int i = 0; i < orderedTablesToCopy.size(); i++) {
    		logger.info(orderedTablesToCopy.get(i));
    	}
    	
    	
    	boolean result = false;
    	for(int i = 0; i < orderedTablesToCopy.size(); i++) {
    		String query = getMsSQLInsertStatement(orderedTablesToCopy.get(i), templateDatabaseName);
    		logger.info(query);
    		try {
				Statement statement = connectionCopy.createStatement();
				statement.executeUpdate(query);
				result = true;
			} catch (SQLException e) {
				connectionCopy.close();
				dropDatabse(connection,targetDatabaseName);
				result = false;
				logger.error(e.getMessage());
				//e.printStackTrace();
			}
    	}   	
    	return result;
    }
    
    public static String getMsSQLInsertStatement(String targetTableName, String templateDatabaseName) throws SQLException {
    	
    	
    	String query = "INSERT INTO [dbo].[" + targetTableName + "]" + " SELECT * FROM [" + templateDatabaseName + "].[dbo].[" + targetTableName + "]" ;
    	return query;
    }
    
    public static void createDatabasePostgresqlWithConnection(Connection connection, String databaseName) {
    	String query = "CREATE DATABASE " + databaseName +";";
    	//System.out.println(query);
    	logger.info(query);
    	
    	Statement statement;
        try
        {
            statement = connection.createStatement();
            statement.execute(query);
           
        }
        catch ( SQLException e )
        {
        	logger.error("Error while creating database.");
        	logger.error(e.getMessage());
            //e.printStackTrace();
            return;
        }
    }

    
    public static boolean copyPostgresqlTablesContent(Connection connection, Connection connectionCopy, String templateDatabaseName,
    		String targetDatabaseName, List<String> tablesToCopy, String databaseType, String mode) throws SQLException {
    	
    	String tableName;
    	logger.info("Inserting data to selected tables...");
    	boolean result = false;
    	
    	
    	List<String> tables = new ArrayList<String>();
    	Statement statement;
    	Statement statementCopy;
		try {
			statement = connection.createStatement();
			statementCopy = connectionCopy.createStatement();
			
			if(mode.equals("force")) {
				tables = tablesToCopy;
			} else {
				tables = collectTablesList(tablesToCopy, connectionCopy, databaseType, "");
			}
			
			
			for(int k = 0 ; k < tables.size(); k++) {
				logger.info(tables.get(k));
				//System.out.println(tables.get(k));
			}
			
			List<PreparedStatement> preparedStatements;
			for(int i = 0; i < tables.size(); i++) {
	    		tableName = tables.get(i);
	    		ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
	    		
	    		
	    		String insert = prepareInsertQueryForTable(rs, tableName);
	    		
	    		preparedStatements = prepareInsertStatement(insert, rs, connectionCopy);
	    		for(int j = 0 ; j < preparedStatements.size(); j++) {
	    		    //System.out.println( preparedStatements.get( j ) );
	    			preparedStatements.get(j).execute();
	    		}	
	    	}
			result = true;
			
		} catch (SQLException e) {
			connectionCopy.close();
			dropDatabse(connection, targetDatabaseName);
			logger.error(e.getMessage());
			//e.printStackTrace();
		}    	
    	return result;
    }
    
    public static List<String> collectTablesList(List<String> tablesNames, Connection connection, String databaseType, String databaseName) {
    	List<String> finalTablesOrder = new ArrayList<String>();
    	List<String> tablesWithForeignKeys = new ArrayList<String>();
    	List<String> normalTables = new ArrayList<String>();
    	
    	for(int i = 0; i < tablesNames.size(); i++) {
    		String table = tablesNames.get(i);
    		if(chceckIfTableContainsForeignKey(table, connection, databaseType, databaseName)) {
    		    //System.out.println( "FK TABLE: " + table );
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
		    
		    //System.out.println(normalTables.size());
		    tmpCounter = normalTables.size();
		   
		    for(int i = 0; i < tablesWithForeignKeys.size(); i++) {
		        
		        String tableName = tablesWithForeignKeys.get(i);
		        List<String> foreignsKeys = getForeignKeysForTable( tableName, connection, databaseType,databaseName);
		        
		        
		        if(checkIfListContainsAllElements( foreignsKeys, normalTables, tableName )) {
		            normalTables.add( tableName );
		            tablesWithForeignKeys.remove( tableName );
		            counter = normalTables.size();
		        }
		    }
		    
		    if(tmpCounter == counter) {
	              for(int k = 0; k < tablesWithForeignKeys.size(); k++) {
	            	  logger.info("SAFE MODE: Possible exception for rows inserted into " + tablesWithForeignKeys.get(k));
	            	  //System.out.println("SAFE MODE: Possible exception for rows inserted into " + tablesWithForeignKeys.get(k));
	                  normalTables.add( tablesWithForeignKeys.get( k ));
	                }
		        endLoop = true;
		    }
		   
		    
		    if(normalTables.size() == tablesNames.size()) {
		        endLoop = true;
		    }
		    
		} while(!endLoop);
		
		return normalTables;

    }
    
    public static List<String> collectTablesWithoutFKException(List<String> tablesNames, Connection connection, String databaseType, String databaseName) {
    	List<String> tablesWithForeignKeys = new ArrayList<String>();
    	List<String> normalTables = new ArrayList<String>();
    	for(int i = 0; i < tablesNames.size(); i++) {
    		String table = tablesNames.get(i);
    		if(chceckIfTableContainsForeignKey(table, connection, databaseType, databaseName)) {
    			//System.out.println("FK TABLE: " + table);
    			tablesWithForeignKeys.add(table);
    		} else {
    			//System.out.println("NORMAL TABLE: " + table);
    			normalTables.add(table);
    		}
    	}
    	
		boolean endLoop = false;
		int counter = 0;
        int tmpCounter = 0;
		do {
			
		    tmpCounter = normalTables.size();
		   
		    for(int i = 0; i < tablesWithForeignKeys.size(); i++) {
		        
		        String tableName = tablesWithForeignKeys.get(i);
		        List<String> foreignsKeys = getForeignKeysForTable( tableName, connection, databaseType,databaseName);
		        //System.out.println("Foreign keys size: " + foreignsKeys.size());
		      
		        if(checkIfListContainsAllElements( foreignsKeys, normalTables, tableName )) {
		            normalTables.add( tableName );
		            tablesWithForeignKeys.remove( tableName );
		            counter = normalTables.size();
		        }
		    }
		    
		    //System.out.println("Tables with foreign keys size: " + tablesWithForeignKeys.size());
		    if(tmpCounter == counter) {
		    	for(int i = 0 ; i < tablesWithForeignKeys.size(); i++) {
		    		logger.info("Table: " + tablesWithForeignKeys.get(i) + " in force mode. Data not inserted becouse of FK constraints.");
		    		//System.out.println("Table: " + tablesWithForeignKeys.get(i) + " in force mode. Data not inserted becouse of FK constraints.");
		    	}
		        endLoop = true;
		    }
		   
		    
		    if(normalTables.size() == tablesNames.size()) {
		        endLoop = true;
		    }
		    
		} while(!endLoop);
		
		return normalTables;
    	
    }
    
    public static boolean checkIfListContainsAllElements(List<String> elements, List<String> list, String tableName) {
        for(int i = 0; i < elements.size(); i++) {
            if(!list.contains( elements.get( i ) )) {
                return false;
            }
        }
        return true;
    }
    
    public static List<String> getForeignKeysForTable(String tableName, Connection connection, String databaseType,
    		String databaseName) {
    	Statement statement;
    	List<String> foreignKeys = new ArrayList<String>();
		try {
			statement = connection.createStatement();
			DatabaseMetaData dm = connection.getMetaData();
			ResultSet tableForeignKeys;
			ResultSet rs;
			if(databaseType.equals("PostgreSQL")) {
				tableForeignKeys = dm.getImportedKeys( null, null, tableName );
				rs = statement.executeQuery("SELECT * FROM " + tableName + " LIMIT 1");
			} else {
				tableForeignKeys = dm.getImportedKeys( databaseName, "dbo", tableName );
				rs = statement.executeQuery("SELECT TOP 1 * FROM " + tableName );
			}
			ResultSetMetaData rsmd = rs.getMetaData();
			String column = "";

			while (tableForeignKeys.next()) {
			    String column_name = tableForeignKeys.getString("PKTABLE_NAME");//
		        foreignKeys.add(column_name);
		    }
			if(foreignKeys.contains( tableName )) {
			    foreignKeys.remove( tableName );
			}
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
		}
    	
		/*System.out.println( "Klucze obce tabeli: " + tableName);
		for(int i = 0; i < foreignKeys.size(); i++) {
		    System.out.println( foreignKeys.get( i ) );
		}
		System.out.println( "" ); */
		return foreignKeys;
    } 
    
    public static boolean chceckIfTableContainsForeignKey(String tableName, Connection connection, 
    		String databaseType, String databaseName) {
    	boolean result = false;
    	try {
			Statement statement = connection.createStatement();
			DatabaseMetaData dm = connection.getMetaData();
			ResultSet tableForeignKeys;
			//System.out.println(databaseType);
			if(databaseType.equals("PostgreSQL")) {
				tableForeignKeys = dm.getImportedKeys(null, null, tableName);
			} else {
				tableForeignKeys = dm.getImportedKeys(databaseName, "dbo", tableName );
			}
			
			String column_name = "";
			List<String> columnNames = new ArrayList<String>();
            while ( tableForeignKeys.next() )
            {
                column_name = tableForeignKeys.getString( "FKTABLE_NAME" );
            }
			if(column_name.length() != 0) {
			    result = true;
			}
			
		} catch (SQLException e) {
			logger.error(e.getMessage());
			//e.printStackTrace();
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
			logger.error(e.getMessage());
			//e.printStackTrace();
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
			logger.error(e.getMessage());
			//e.printStackTrace();
		}	
		   String sql = "INSERT INTO " + tableName + " ("
		              + columnNames
		              + ") VALUES ("
		              + bindVariables
		              + ");";
    	return sql;
    }
    
    public static boolean getBooleanFromFile(String propertiesPath, String param) {
    	FileInputStream inputStream = null;
    	boolean result = false;
		try {
			Properties prop = new Properties();
			inputStream = new FileInputStream(propertiesPath);

			prop.load(inputStream);
			
			// get the property value and print it out
			String tmp = "";
			tmp = prop.getProperty(param);
			if(tmp.equals("true")) {
				result = true;
			} else {
				result = false;
			}

		} catch (Exception e) {
			logger.error("Exception" + e);
			//System.out.println("Exception: " + e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				//e.printStackTrace();
				logger.error(e.getMessage());
			}
		}
		return result;
    }
    
    
    public static boolean getIntegratedSecurity(String propertiesPath) {
    	FileInputStream inputStream = null;
    	boolean result = false;
		try {
			Properties prop = new Properties();
			inputStream = new FileInputStream(propertiesPath);

			prop.load(inputStream);
			
			// get the property value and print it out
			String tmp = "";
			tmp = prop.getProperty("mssql.integratedSecurity");
			if(tmp.equals("true")) {
				result = true;
			} else {
				result = false;
			}

		} catch (Exception e) {
			logger.error("Exception: " + e);
			//System.out.println("Exception: " + e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
				//e.printStackTrace();
			}
		}
		return result;
    }
    
    public static String getPropertyFromFile(String filePath, String propertyName) {
    	
    	FileInputStream inputStream = null;
    	String result = "";
		try {
			Properties prop = new Properties();
			inputStream = new FileInputStream(filePath);

			prop.load(inputStream);
			
			result = prop.getProperty(propertyName);

		} catch (Exception e) {
			logger.error("Exception: " + e);
			//System.out.println("Exception: " + e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
				//e.printStackTrace();
			}
		}
		return result;
    }
    
    
    public static List<String> readUnusedTablesNamesFromProperties(String filePath) {
    	List<String> unusedTablesNames = new ArrayList<String>();
    	
    	FileInputStream inputStream = null;
		try {
			Properties prop = new Properties();
			inputStream = new FileInputStream(filePath);

			prop.load(inputStream);
			
			Enumeration<?> properties = prop.propertyNames();
			while(properties.hasMoreElements()){
				String propertyName = (String) properties.nextElement();
				if(propertyName.contains("unusedTable.")) {
					String result = prop.getProperty(propertyName);
					unusedTablesNames.add(result);
				}
			}
		} catch (Exception e) {
			logger.error("Exception: " + e);
			//System.out.println("Exception: " + e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
				//e.printStackTrace();
			}
		}
		return unusedTablesNames;
    }
    
    public static List<String> readUnusedSequencesNamesFromProperties(String filePath) {
        List<String> unusedSequencesNames = new ArrayList<String>();
        
        FileInputStream inputStream = null;
        try {
            Properties prop = new Properties();
            inputStream = new FileInputStream(filePath);

            prop.load(inputStream);
            
            Enumeration<?> properties = prop.propertyNames();
            while(properties.hasMoreElements()){
                String propertyName = (String) properties.nextElement();
                if(propertyName.contains("unusedSequence.")) {
                    String result = prop.getProperty(propertyName);
                    unusedSequencesNames.add(result);
                }
            }
        } catch (Exception e) {
            logger.error("Exception: " + e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        return unusedSequencesNames;
    }
    
    public static List<String> readUnusedMergeSequencesNamesFromProperties(String filePath) {
        List<String> unusedMergeSequencesNames = new ArrayList<String>();
        
        FileInputStream inputStream = null;
        try {
            Properties prop = new Properties();
            inputStream = new FileInputStream(filePath);

            prop.load(inputStream);
            
            Enumeration<?> properties = prop.propertyNames();
            while(properties.hasMoreElements()){
                String propertyName = (String) properties.nextElement();
                if(propertyName.contains("unusedMergeSequence.")) {
                    String result = prop.getProperty(propertyName);
                    unusedMergeSequencesNames.add(result);
                }
            }
        } catch (Exception e) {
            logger.error("Exception: " + e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        return unusedMergeSequencesNames;
    }
    
    public static List<String> readMergeTablesNamesFromProperties(String filePath) {
    	List<String> unusedTablesNames = new ArrayList<String>();
    	
    	FileInputStream inputStream = null;
		try {
			Properties prop = new Properties();
			inputStream = new FileInputStream(filePath);

			prop.load(inputStream);
			
			Enumeration<?> properties = prop.propertyNames();
			while(properties.hasMoreElements()){
				String propertyName = (String) properties.nextElement();
				if(propertyName.contains("mergeTable.")) {
					String result = prop.getProperty(propertyName);
					unusedTablesNames.add(result);
				}
			}
		} catch (Exception e) {
			logger.error("Exception: " + e);
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				logger.error(e.getMessage());
				//e.printStackTrace();
			}
		}
		return unusedTablesNames;
    }
    

}
