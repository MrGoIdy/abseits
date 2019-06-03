# -*- coding: utf-8 -*-
import requests
import datetime
from parsel import Selector
import mysql.connector

connection = mysql.connector.connect(host='5.132.159.203', database='Test_Alex', user='alex', password='NastiViennaMinsk2018_')
cursor = connection.cursor(prepared=True)
try:

    translationTable = str.maketrans("ÄäÖöÜüß–", "AaOoUuB-")   
    print("Loading data...")
    fund_page = requests.get("https://abseits.at/fernsehprogramm/")
    selTables = Selector(fund_page.text)
    file = open('abseits_output_'+str(datetime.date.today())+'.txt', 'w')
    dates = {}
    dateCounterLoading = 0
    print("Loading dates...")
    for i in range(0,13):
        try:
            date = selTables.xpath('//p//text()')[i].get()
            dateToAdd = date.split(',')[1].strip()[:-1]
            if(len(date)<5):
                continue
            elif(len(dateToAdd)!=5):
                continue
            else:
                nextParagraph = selTables.xpath('//p//text()')[i+1].get().strip()
                if(nextParagraph!='–'):
                    dates[dateCounterLoading]=dateToAdd
                    dateCounterLoading=dateCounterLoading+1
        except:
            none=""
    print("Processing data...")
    for tableCounter in range(0,len(dates)):
       
        table = selTables.xpath('//table')[tableCounter].get()
        print("---Date/Channel/Teams/League---")
        for trCounter in range(0,len(Selector(table).xpath('//tr').getall())):
            try:
                print("Table "+str(tableCounter)+"/Tr "+str(trCounter))
                tr = Selector(table).xpath('//tr')[trCounter].get()
                selTr = Selector(tr)
                time = selTr.xpath('//td//text()')[0].get().strip().translate(translationTable)
                channel = selTr.xpath('//td//text()')[1].get().strip().translate(translationTable)
                try:
                    team1 = selTr.xpath('//td//text()')[2].get().split('–')[0].strip().translate(translationTable)
                    team2 = selTr.xpath('//td//text()')[2].get().split('–')[1].split('(')[0].strip().translate(translationTable)
                except:
                    team1 = "No info on site"
                    team2 = "No info on site"
                leagues = selTr.xpath('//td//text()')[2].get().split('(')[1].split(')')[0].strip().translate(translationTable)
                channelsCount = channel.count("/")
               
                if(channelsCount>0):
                    for counter in range(0,channelsCount+1):
                        print(time+"/"+channel.split("/")[counter]+"/"+team1+"/"+team2+"/"+leagues)
                        try:
                            cursor = connection.cursor(prepared=True)
                            sql_insert_query = "INSERT INTO `abseits_fernsehprogramm` (`Date`, `Time`, `Channel`, `Team1`, `Team2`, `League`) VALUES (%s,%s,%s,%s,%s,%s)"
                            insert_tuple = (dates[tableCounter], time, channel.split("/")[counter], team1, team2, leagues)
                            result  = cursor.execute(sql_insert_query, insert_tuple)
                            connection.commit()
                            print ("Record inserted successfully into table")
                        except Exception as ex:
                            print(ex)
               
                
                else:
                    print(time+"/"+channel+"/"+team1+"/"+team2+"/"+leagues)
                    try:
                        cursor = connection.cursor(prepared=True)
                        sql_insert_query = "INSERT INTO `abseits_fernsehprogramm` (`Date`, `Time`, `Channel`, `Team1`, `Team2`, `League`) VALUES (%s,%s,%s,%s,%s,%s)"
                        insert_tuple = (dates[tableCounter], time, channel, team1, team2, leagues)
                        result  = cursor.execute(sql_insert_query, insert_tuple)
                        connection.commit()
                        print ("Record inserted successfully into table")
                    except Exception as ex:
                        print(ex)
            except:
                print("Error while work with table "+str(tableCounter)+" row "+str(trCounter))              
except Exception as ex:
    print(ex)
finally:
    if(connection.is_connected()):
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
print("End.")

