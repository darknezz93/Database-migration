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
		msg += "database_type operation_type";
		
		return msg;
	}

}
