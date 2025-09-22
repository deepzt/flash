import pandas as pd
import os
from os import listdir
from os.path import isfile, join
import datetime as dt
import fast_to_sql as fts
import pyodbc

connectionString = f'DRIVER={{SQL Server}};server;DB;UID;PWD'
folder = 'C:/AnalyticsProject/Meteo Save Data/Raw_Data/'

conn = pyodbc.connect(connectionString)
cursor = conn.cursor()

def ValidateGracePeriod():
    graceDays = 12
    todayDate = dt.datetime.now()
    isGracePeriod = 'N'
    
    # Quarter change months: Nov (Q4->Q1), Feb (Q1->Q2), May (Q2->Q3), Aug (Q3->Q4)
    quarter_change_months = [11, 2, 5, 8]
    
    if todayDate.month in quarter_change_months:
        # Grace period starts from 5th of the month during quarter changes
        if todayDate.day >= 13 and todayDate.day <= (13 + graceDays - 1):
            isGracePeriod = "Y"
    
    return isGracePeriod

def ValidateQuarter():
    previous = ""
    current = ""
    todayDate = dt.datetime.now()

    if todayDate.month == 11 or todayDate.month == 12:
        yearInNumber = int(todayDate.strftime("%y")) + 1
        previous = "FY" + todayDate.strftime("%y")
        current = "FY" + str(yearInNumber)
    else:
        previous = "FY" + todayDate.strftime("%y")
        current = "FY" + todayDate.strftime("%y")

    if todayDate.month == 11 or todayDate.month == 12 or todayDate.month == 1:
        previous += "Q4"
        current += "Q1"
    elif todayDate.month == 2 or todayDate.month == 3 or todayDate.month == 4:
        previous += "Q1"
        current += "Q2"
    elif todayDate.month == 5 or todayDate.month == 6 or todayDate.month == 7:
        previous += "Q2"
        current += "Q3"
    elif todayDate.month == 8 or todayDate.month == 9 or todayDate.month == 10:
        previous += "Q3"
        current += "Q4"

    return [previous, current]


isGracePeriod = ValidateGracePeriod()
quarters = ValidateQuarter()

files = [f for f in listdir(folder) if isfile(join(folder, f))]
print(files)
print(f"Grace period: {isGracePeriod}")
if isGracePeriod == 'Y':
    if len(files) < 2:
        print("Please introduce 2 Quarters....\n")
        #here we need execute a SP with the alert just in case the process is complete automatic
    else:
        for file in files:
            dateInFile = file.split(".")[1]
            ddate = dateInFile.split("-")
            dateInFile = dt.date(int(ddate[0]),int(ddate[1]),int(ddate[2]))

            print(folder + file)
            data = pd.read_excel(folder + file)

            valor_a2 = data.iloc[1, 2]        
            print(f"Found the PERIOD in the file: {str(valor_a2)}\n")

            if str(quarters[1]) == str(valor_a2):
                print(f"Comparing data of {str(quarters[1])} with {str(valor_a2)}")
                table_Name = "METEO_REPORT_CURRENT"
                print(f"Entering the {str(valor_a2)} data in current quarter table: {table_Name}\n")

            elif str(quarters[0]) == str(valor_a2):
                print(f"Comparing data of {str(quarters[0])} with {str(valor_a2)}")
                table_Name = "METEO_REPORT_PREVIOUS"
                print(f"Entering the {str(valor_a2)} data in previous quarter table: {table_Name}\n")

            else:
                print(f"Warning: {file} has an unexpected quarter value: {valor_a2}\n")
                continue

            print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' | df Imported Starts\n')

            data['PARTNER_NAME'] = data['PARTNER_NAME'].replace({'"' : ' '})
            
            query = fts.fast_to_sql(data, table_Name,conn, custom={
                                    "[SCHEME]":"nvarchar(255) NULL",
                                    "[PL]":"nvarchar(255) NULL",
                                    "[PERIOD]":"nvarchar(255) NULL",
                                    "[SCHEME_REGION]":"nvarchar(255) NULL",
                                    "[PARTNER_ID]":"nvarchar(255) NULL",
                                    "[PARTY_ID]":"nvarchar(255) NULL",
                                    "[PARTNER_TYPE]":"nvarchar(255) NULL",
                                    })
            
            print(query)
            conn.commit()
            print(f"Execution for: {table_Name} complete.")
            print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' | df Imported Successfully\n')

        sql_exec = 'SET NOCOUNT ON; EXEC [dbo].[SP_METEO_TEST]' + "'" + str(dateInFile) + "','" + str(isGracePeriod) + "','" + str(quarters[0]) + "','" + str(quarters[1]) + "'"  # type: ignore
        cursor.execute(sql_exec)
        conn.commit()

else:
    if len(files) > 1:
        print("Out of Grace Period. Introduce only 1 file....\n")
        exit()
    else:
        for file in files:
            dateInFile = file.split(".")[1]
            ddate = dateInFile.split("-")
            dateInFile = dt.date(int(ddate[0]),int(ddate[1]),int(ddate[2]))

            print(folder + file)
            data = pd.read_excel(folder + file)        
        
            print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' | df Imported Starts')

            data['PARTNER_NAME'] = data['PARTNER_NAME'].replace({'"' : ' '})
            
            query = fts.fast_to_sql(data, "METEO_REPORT_CURRENT", conn, custom={
                                    "[SCHEME]":"nvarchar(255) NULL",
                                    "[PL]":"nvarchar(255) NULL",
                                    "[PERIOD]":"nvarchar(255) NULL",
                                    "[SCHEME_REGION]":"nvarchar(255) NULL",
                                    "[PARTNER_ID]":"nvarchar(255) NULL",
                                    "[PARTY_ID]":"nvarchar(255) NULL",
                                    "[PARTNER_TYPE]":"nvarchar(255) NULL",
                                    })

            print("Normal execution successfully")
            print(query)
            conn.commit()

            print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' | df Imported Successfully')
            sql_exec = 'SET NOCOUNT ON; EXEC [dbo].[SP_METEO_TEST]' + "'" + str(dateInFile) + "','" + str(isGracePeriod) + "','" + str(quarters[0]) + "','" + str(quarters[1]) + "'" 
            cursor.execute(sql_exec)
            conn.commit()

# ONLY ONE EXECUTION OF ALERTS PER EXECUTION OF PROCESS
###################################### ACTIVE THE NEXT 3 LINES ONCE THIS PROJECT IS RUNNING IN PRODUCTION ###########################
sql_exec = 'SET NOCOUNT ON; EXEC [dbo].[SP_METEO_DAILY_ALERTS]'
cursor.execute(sql_exec)
conn.commit()