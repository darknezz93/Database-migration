package com.database.migration;

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
		msg += "Example : postgresql clone force\n";
		msg += "\n";
		
		msg += "--- MERGING DATABASE --- \n";
		msg += "database_type operation_type mode_type schema-only(optional)\n";
		msg += "Example : mssql merge force schema-only\n";
		msg += "\n";
		
		msg += "--- EXPORTING DATABASE --- \n";
		msg += "export_type zip_path\n";
		msg += "Example : export-postgresql C:\\Users\\Adam\\Desktop\\dvdrental.zip\n";
		msg += "\n";
		
		
		msg += "--- IMPORTING DATABASE --- \n";
		msg += "import_type zip_path\n";
		msg += "Example : import-postgresql C:\\Users\\Adam\\Desktop\\dvdrental.zip\n";
		msg += "\n\n";
		
		msg += "--- CONFIGURATION ---\n\n";
		msg += "1. Create SuncodeDatabaseMigration directory in your home direcotry\n";
		msg += "2. Create postgresql.properties and mssql.properties files in SuncodeDatabaseMigration\n";
		msg += "3. Create unusedTables.properties and mergeTables files in SuncodeDatabaseMigration\n";
		msg += "4. Add proper databases properties to created files\n\n";
		
		msg += "--- PROPERTIES FILES CONTENT ---\n\n";
		msg += "Example properties of postgresql.properties file : \n\n";
		msg += "host=localhost \n";
		msg += "port=5432\n";
		msg += "databaseName=exampleDB\n";
		msg += "targetDatabaseName=exampleBbCopy\n";
		msg += "adminDatabaseName=postgres\n";
		msg += "restoreDatabaseName=restoreDBName\n";
		msg += "userName=pguser\n";
		msg += "password=pguser\n";
		msg += "pg_dumpPath=C:\\ProgramFiles\\PostgreSQL\\9.5\\bin\\pg_dump \n";
		msg += "psqlPath=C:\\ProgramFiles\\PostgreSQL\\9.5\\bin\\psql \n";
		msg += "secondDatabaseName=dvdrental\n";
		msg += "secondUserName=pguser\n";
		msg += "secondPassword=pguser\n\n\n";
		
		msg += "Example properties of mssql.properties file : \n\n";
		msg += "host=localhost\n";
		msg += "port=1433\n";
		msg += "databaseName=exampleDB\n";
	    msg += "targetDatabaseName=exampleDbCopy\n";
		msg += "restoreDatabaseName=restoreDBName\n";
		msg += "userName=user\n";
	    msg += "password=password\n";
		msg += "integratedSecurity=true\n";
		msg += "adminDatabaseName=pubs\n";
		msg += "secondDatabaseName=Northwind\n";
		msg += "secondUserName=adam\n";
		msg += "secondPassword=password\n\n\n";
		
		msg += "Example properties of unusedTables.properties file : \n\n";
		msg += "table1=city\n";
		msg += "table2=customer\n\n\n";
		
		msg += "Example properties of mergeTables.properties file : \n\n";
		msg += "table1=rental\n";
		msg += "table2=cars\n";
		
		
		
		return msg;
	}

}
