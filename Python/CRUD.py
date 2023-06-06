#importing necessary libraries
import pyodbc
import datetime
import pandas as pd
from pandas import ExcelWriter
 
#making connection with database
class pysqlconnect:
    def __init__(self,connectionString:str) -> None:
        """ Initialize the Server class
        Parameters:
        connectionString (str): The connection string to the database.

        """
        self.connection = None
        self.cursor = None
        try:
            self.connectionString=connectionString
            self.connection = pyodbc.connect(self.connectionString)
            self.cursor = self.connection.cursor()
            print('Connection with database is established')
        except Exception as e:
            print(f'Connection Error: {str(e)}')

    #creating the database

    def create(self,tableName:str,columns:dict) -> None:
        """ Insert data into a table
        """
        try:
            colName=", ".join(columns.keys())
            valData=str([val for val in columns.values()])[1:-1]
            query=f"INSERT INTO {tableName} ({colName}) VALUES ({valData})"
            self.cursor.execute(query)
            self.connection.commit()
            print(f"Row insterted into {tableName} successfully")

        except Exception as e:
            self.connection.rollback()
            print(f'Insertion Error: {str(e)}')

    #reading from the database
    def read(self,tableName:str,columns:list=None,where:str=None) -> list:
        """
        Read data from table

        """
        try:
            colName=", ".join(columns) if columns else "*"
            whereClause=f" WHERE {where}" if where else ""
            query=f"SELECT {colName} FROM {tableName}{whereClause}"
            df=pd.read_sql(query,self.connection)
            time=datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            self.exportToExcel(df,f"results/excel/{time}-{tableName}.xlsx")
            self.exportToCSV(df,f"results/csv/{time}-{tableName}.csv")
            print(f"Data from {tableName} table is read successfully")

        except Exception as e:
            print(f'Reading Error: {str(e)}')

    #updatuing the database
    def update(self,tableName:str,columns:dict,where:str=None)->None:
        """
        Update data in table
      
        """
        try:
            set_str = ", ".join([f"{key} = '{val}'" for key, val in columns.items()])
            whereClause = f"WHERE {where}" if where else ""
            query=f"UPDATE {tableName} SET {set_str} {whereClause}"
            self.cursor.execute(query)
            self.connection.commit()
            print(f"Data in {tableName} table is updated successfully")

        except Exception as e:
            self.connection.rollback()
            print(f'Updation Error: {str(e)}')

    #deleting from database
    def delete(self,tableName:str,where:str=None)->None:
        """
        Delete data from table
        
        """
        try:
            whereClause=f"WHERE {where}" if where else ""
            query=f"DELETE FROM {tableName} {whereClause}"
            self.cursor.execute(query)  
            self.connection.commit()
            print(f"Data from {tableName} table is deleted successfully")

        except Exception as e:
            self.connection.rollback()
            print(f'Deletion Error: {str(e)}')

    def exportToExcel(self,df:pd.DataFrame,filename:str)->None:
        """ 
        Export data to excel
       
        """
        try:
            writer=ExcelWriter(filename)
            df.to_excel(writer,'Sheet1',index=False)
            writer.save()
            print(f"Data is exported to {filename} successfully")

        except Exception as e:
            print(f'Exporting to Excel Error: {str(e)}')

    def exportToCSV(self,df:pd.DataFrame,filename:str)->None:
        """ Export data to csv

        """
        try:
            df.to_csv(filename,index=False)
            print(f"Data is exported to {filename} successfully")
        except Exception as e:
            print(f'Exporting to CSV Error: {str(e)}')

    def __del__(self):
        """ Close the connection to the database.
        """
        if self.connection:
            try:
                self.cursor.close()
                self.connection.close()
                print('Connection with database is closed')

            except Exception as e:
                print(f' Error: {str(e)}')
        

        
connectionString= 'Driver={SQL Server};Server=LAPTOP-U9U11BTI\SQLEXPRESS;Database=PersonDB;Trusted_Connection=yes;'
dbobj=pysqlconnect(connectionString)

# dbobj.create('person',{"Name":"nandan","age":20})
# dbobj.delete('person',"Name='nandan')
# dbobj.update('person',{"Name":"nandan","age":20},"Name='singh')# dbobj.read('Books',["Name","Price"])













        