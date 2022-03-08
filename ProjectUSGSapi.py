import datetime, folium, os
from folium.plugins import MarkerCluster
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from datetime import datetime, timedelta
import pandas as pd
import json, requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import matplotlib.pyplot as plt
import sqlite3

# to supress the warning due to unvarified SSl during HTTP request.
from requests_futures.sessions import FuturesSession

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

pathToWorkingFolder = "/Users/vsm/Downloads/"
# Path to the working folder
os.chdir(pathToWorkingFolder)
print('Current Working Dir:', os.getcwd())


def CTT(processName, startTime, endTime):
    elaspsedTime = str(endTime - startTime)
    elaspsedTime = elaspsedTime.split(':')
    return 'Total time taken to complete %s : %s Mins %s Seconds' % (processName, elaspsedTime[1], elaspsedTime[2][0:2])



def DftoRDBMS(df):
    # df = pd.DataFrame(df)
    df = df.set_index('ID')
    conn = sqlite3.connect('EVENTDB.db')
    c = conn.cursor()

    c.execute('DROP TABLE USDATA')
    '''
    Questions 3 & 4 - Design the database Objects response and insert data from data frame
    '''
    c.execute('CREATE TABLE USDATA (ID, MAG, PLACE, TIME, UPDATED, TZ, URL, FELT, CDI, MMI, ALERT, STATUS, TSUNAMI, SIG, NET, SOURCES, NST, DMIN, RMS, GAP, MAGTYPE, TITLE, LONGITUDE, LATITUDE, DEPTH)')
    conn.commit()
    c.execute('CREATE TABLE USDATA_TEMP AS SELECT * FROM USDATA')
    conn.commit()
    df.to_sql('USDATA_TEMP', conn, if_exists='replace', index=True)

    def display():
        for row in c.fetchall():
            print(row)

    try:
        c.execute('''
                SELECT DISTINCT STRFTIME('%m',TIME) as DATE FROM USDATA
                ''')
        display()

        c.execute('DELETE FROM USDATA WHERE ID IN (SELECT ID FROM USDATA_TEMP)')

        df.to_sql('USDATA', conn, if_exists='append', index=True)
        # t.commit()
        '''
        Questions 6 - Analysis on the biggest earthquake in 2017
        '''
        print("############ MAX MAGNITUDE ##################")
        c.execute('''
        SELECT * FROM USDATA WHERE MAG IN (SELECT MAX(MAG) FROM USDATA)
        ''')
        display()

        print("############ MAX MAGNITUDE EACH YEAR ##################")
        c.execute('''
        SELECT SUBSTR(TIME,1,4) as DATE,MAX(MAG) FROM USDATA 
        GROUP BY SUBSTR(TIME,1,4)
        ''')
        display()
        '''
        Questions 7 - Analysis on the Range of Magnitude each hour
        '''
        print("############ RANGE OF MAGNITUDE EACH HOUR ##################")
        c.execute('''
        SELECT STRFTIME('%H',TIME) as DATE,
        CASE WHEN MAG < 1 THEN COUNT(MAG) END AS COUNT_LT_1 ,
        CASE WHEN MAG BETWEEN 1 and 2 THEN COUNT(MAG) END AS COUNT_1_2 ,
        CASE WHEN MAG BETWEEN 2 and 3 THEN COUNT(MAG) END AS COUNT_2_3 ,
        CASE WHEN MAG BETWEEN 4 and 5 THEN COUNT(MAG) END AS COUNT_4_5 ,
        CASE WHEN MAG BETWEEN 5 and 6 THEN COUNT(MAG) END AS COUNT_4_5 ,
        CASE WHEN MAG > 6 THEN COUNT(MAG) END AS COUNT_GT_6
        FROM USDATA 
        GROUP BY STRFTIME('%H',TIME)
        ''')
        display()
    except:
        print("ERROR'ED OUT SQLITE")

    finally:
        c.execute('''
        DROP TABLE USDATA_TEMP
        ''')

class USData:
    def getDateList(self, start, end):
        '''
        Questions 5 - Adding an incremental fetch for the API
        '''
        startTime = start
        endTime = end
        startDateList = [startTime]
        endDateList = []
        dateFormat = '%Y-%m-%d'
        startDate = datetime.strptime(startTime, dateFormat)
        endDate = datetime.strptime(endTime, dateFormat)
        datelist = pd.date_range(startDate, endDate, freq='M')

        for element in datelist:
            element = element + timedelta(days=1)
            startDateList.append(element.strftime(dateFormat))
            endDateList.append(element.strftime(dateFormat))
        return startDateList, endDateList


    def getProcessedData(self, startDate, endDate):
        try:
            isProcessedDataAvailable = False
            startTime = datetime.now()
            startDateList, endDateList = self.getDateList(startDate, endDate)
            '''
            ## Question 1 - Query all the events the occurred during 2017
            '''
            requestUrls = [
                'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventtype=earthquake&starttime=' + st + '&endtime=' + end
                for st, end in zip(startDateList, endDateList)]
            session = FuturesSession(max_workers=len(requestUrls))

            Bd = pd.DataFrame()

            for i in range(len(requestUrls)):
                response = session.get(requestUrls[i], verify=False).result().content
                '''
                Questions 2 - Read all the JSON response from API
                '''
                df = pd.json_normalize(json.loads(response)['features'])
                df[['Longitude', 'Latitude', 'Depth']] = pd.DataFrame(df['geometry.coordinates'].values.tolist(),
                                                                      index=df.index)
                df = df.rename(columns=lambda x: x.replace('geometry.', '').replace('properties.', ''))
                df.drop(columns=['coordinates', 'type', 'code', 'detail', 'ids', 'types'], inplace=True)
                df.time = pd.to_datetime(df.time, unit='ms')
                df.updated = pd.to_datetime(df.updated, unit='ms')
                df = df.rename(str.upper, axis='columns')
                Bd = Bd.append(df)
            print(CTT('parallel EQ data downloading', startTime, datetime.now()))
            self.RefinedData = Bd
            print(self.RefinedData.count())
            isProcessedDataAvailable = True
            return isProcessedDataAvailable
        except:
            return isProcessedDataAvailable


startDate = '2017-01-01'
endDate = '2017-12-31'

if (__name__ == '__main__'):
    try:

        startTime = datetime.now()
        objEQData = USData()
        isProcessedDataAvailable = objEQData.getProcessedData(startDate, endDate)

        if (isProcessedDataAvailable):
            startTime = datetime.now()
            print('JSON Data Processed Successfully To DataFrame')
            processedDataFrame = objEQData.RefinedData
            processedDataFrame.to_csv('USGSData.csv')
            print(CTT('data processing', startTime, datetime.now()))
            # Df to DB
            startTime = datetime.now()
            DftoRDBMS(processedDataFrame)
            print(CTT('database processing completed', startTime, datetime.now()))

    except Exception as e:
        print('Error occured in the main process. Please confirm the details and re-process')
        pass