package com.suncode.migration;

public class Help {
	
	private String message;

	public String getMessage() {
		return message;
	}

	public Help() {
		this.message = generateHelpMessage();
	}
	
	public void showMessage() {
		System.out.println(this.message);
	}
	
	private String generateHelpMessage() {
		String msg = "";
		msg += "LIST OF ARGUMENTS: \n";
		msg += "-------------------------------\n";
		msg += "DATABASE_TYPE: type of database\n";
		msg += "[allowed values: postgresql , mssql]\n";
		msg += "  *postgresql: PostgreSQL database\n";
		msg += "  *mssql: Microsoft SQL Server Database\n";
		msg += "\n";
		msg += "OPERATION_TYPE: type of performed operation\n";
		msg += "[allowed values: clone, merge]\n";
		msg += "  *clone: clone selected database\n";
		msg += "  *merge: merge selected database with other one\n";
		msg += "\n";
		msg += "OPERATION_MODE: mode of performed operation\n";
		msg += "[allowed values: safe, force]\n";
		msg += "  *safe: used while cloning/merging database. safe mode means that when foreign keys attributes \n";
		msg += "         are violated application will throw an exception and database cloning will be \n";
		msg += "         terminated\n";
		msg += "  *force: used while cloning/merging database. force mode means that when foreign keys attributes \n";
		msg += "          are violated application will copy only schema of table without data\n";
		msg += "\n";
		msg += "EXPORT_TYPE: type of exported database \n";
		msg += "[allowed values: export-postgresql, export-mssql]\n";
		msg += "  *export-postgresql: exports PostgreSQL database\n";
		msg += "  *export-mssql: exports mssql database\n";
		msg += "\n";
		msg += "IMPORT_TYPE: type of imported database \n";
		msg += "[allowed values: import-postgresql, import-mssql]\n";
		msg += "  *import-postgresql: imports PostgreSQL database\n";
		msg += "  *import-mssql: imports Microsoft SQL Server database\n";
		msg += "\n";
		
		msg += "--- COPYING DATABASE --- \n";
		msg += "database_type operation_type mode_type \n";
		msg += "Example : postgresql clone force C:\\database.properties\n";
		msg += "\n";
		
		msg += "--- MERGING DATABASE --- \n";
		msg += "database_type operation_type mode_type schema-only(optional)\n";
		msg += "Example : mssql merge force schema-only C:/database.properties\n";
		msg += "\n";
		
		msg += "--- EXPORTING DATABASE --- \n";
		msg += "export_type zip_path\n";
		msg += "Description: Exporting database schema with tables content except content of unusedTables from properties file.\n";
		msg += "Exports all sequences except unusedSequences from properties file.";
		msg += "Example : export-postgresql /home/temp/file.zip  /home/temp/database.properties\n";
		msg += "Exporting database with :host, :port, :databaseName, :userName and :password properties.\n";
		msg += "\n";
		
		
		msg += "--- IMPORTING DATABASE --- \n";
		msg += "import_type zip_path\n";
		msg += "Example : import-postgresql /home/temp/file.zip  /home/temp/database.properties\n";
		msg += "Importing database with :host, :port, :adminUserName, :adminPassword properties to :restoreDatabaseName\n";
		msg += "\n\n";
		
		msg += "--- CONFIGURATION ---\n\n";
		msg += "1. Create database.properties file\n";
		msg += "2. Add proper databases properties to created files\n\n";
		
		msg += "--- PROPERTIES FILES CONTENT ---\n\n";
		msg += "Example properties of database.properties file : \n\n";
		msg += "#PostgreSQL\n";
		msg += "postgresql.host=localhost\n";
		msg += "postgresql.port=5432\n";
		msg += "postgresql.targetHost=localhost\n";
		msg += "postgresql.targetPort=5432\n";
		msg += "postgresql.databaseName=plusworkflowdev_copy\n";
		msg += "postgresql.targetDatabaseName=tempdb\n";
		
		msg += "postgresql.adminDatabaseName=postgres\n";
		msg += "postgresql.adminUserName=pguser\n";
		msg += "postgresql.adminPassword=pguser\n";
		
		msg += "postgresql.restoreDatabaseName=skopiowana3000\n";
		msg += "postgresql.userName=pguser\n";
		msg += "postgresql.password=pguser\n";
		msg += "postgresql.pg_dumpPath=C:\\ProgramFiles\\PostgreSQL\\9.5\\bin\\pg_dump\n";
		msg += "postgresql.psqlPath=C:\\ProgramFiles\\PostgreSQL\\9.5\\bin\\psql\n";
		msg += "postgresql.updateSequences=false\n\n";
		
		msg += "#ustawienie na false powoduje ustawienie wszystkich mergowanych sekwencji na 1 ( nie trzeba podawac w\n"; 
		msg += "#unusedMergeSequencesNames), gdy chcemy aby uwzględniło wartości z unusedMergeSequences wartość musi być zawsze true\n";
		msg += "postgresql.createMergeSequences=false\n\n";
		msg += "#Second\n";
		msg += "postgresql.secondDatabaseName=dvdrental_copy\n\n";


		msg += "#MsSQL\n";
		msg += "mssql.host=localhost\n";
		msg += "mssql.port=1433\n";
		msg += "mssql.targetHost=localhost\n";
		msg += "mssql.targetPort=1433\n";
		msg += "mssql.databaseName=pubs_copy\n";
		msg += "mssql.targetDatabaseName=temp_pubs\n";
		msg += "mssql.restoreDatabaseName=skopiowana3000\n";
		msg += "mssql.userName=adam\n";
		msg += "mssql.password=password\n";
		msg += "mssql.integratedSecurity=true\n";
		msg += "mssql.adminDatabaseName=pubs\n";
		msg += "#Second\n";
		msg += "mssql.secondDatabaseName=trunk\n\n";

		msg += "#Unused Tables\n";
		msg += "unusedTable.table1=jobs\n";
		msg += "unusedTable.table2=address\n\n";
		
		msg += "#Unused Sequences\n";
		msg += "unusedSequence.sequence1=pm_as_status_id_seq\n";
		msg += "unusedSequence.sequence2=pm_dashboard_gadgetprop_id\n\n";

		msg += "#Merge Tables\n";
		msg += "mergeTable.table1=Point\n";
		msg += "mergeTable.table2=User\n\n";
		
		msg += "#Unused Merge Tables sequences\n";
		msg += "unusedMergeSequence.sequence1=pm_dashboard_gadgetprop_id\n";
		msg += "unusedMergeSequence.sequence2=pm_processdata_id_seq\n";
		
		return msg;
	}

}
