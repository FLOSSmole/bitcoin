# -*- coding: utf-8 -*-

## This program is free software; you can redistribute it
## and/or modify it under the same terms as Perl.
## Please see the Perl Artistic License 2.0.
## 
## Copyright (C) 2004-2016 Megan Squire <msquire@elon.edu>
##
## We're working on this at http://flossmole.org - Come help us build 
## an open and accessible repository for data and analyses for open
## source projects.
##
## If you use this code or data for preparing an academic paper please
## provide a citation to 
##
## Howison, J., Conklin, M., & Crowston, K. (2006). FLOSSmole: 
## A collaborative repository for FLOSS research data and analyses. 
## International Journal of Information Technology and Web Engineering, 1(3), 17–26.
##
## and
##
## FLOSSmole: a project to provide research access to 
## data and analyses of open source projects.  
## Available at http://flossmole.org 
##
################################################################
# usage:
# 2parseBitcoinDevIRCLogs.py 51313 password
# purpose: 
# take file from disk and parse out interesting bits, write those to db
################################################################

import pymysql
from bs4 import BeautifulSoup
import codecs
import sys

datasource_id = str(sys.argv[1])
pw = str(sys.argv[2])
forge_id = 66
fileLoc = ''

# Open local database connection 1
db1 = pymysql.connect(host="grid6.cs.elon.edu",
                      user="megan",
                      passwd=pw,
                      db="ossmole_merged",
                      use_unicode=True,
                      charset="utf8")
cursor1 = db1.cursor()



# Open local database connection 2
db2 = pymysql.connect(host="grid6.cs.elon.edu",
                      user="megan",
                      passwd=pw,
                      db="bitcoin",
                      use_unicode=True,
                      charset="utf8")
cursor2 = db2.cursor()

# Open remote database connection 3
db3 = pymysql.connect(host="flossdata.syr.edu",
                      user="megan",
                      passwd=pw,
                      db="bitcoin",
                      use_unicode=True,
                      charset="utf8")
cursor3 = db3.cursor()

# get the list of all files to parse
cursor1.execute('SELECT datasource_id, comments \
                FROM datasources \
                WHERE datasource_id >= %s \
                AND forge_id= %s', (datasource_id, forge_id))
    
rows = cursor1.fetchall()
for row in rows:
    current_ds = row[0]
    fileLoc = row[1]
    linecounter = 0;
    
    date_of_entry = fileLoc.split('/',1)[1]
    time_of_entry = ''
    unix_time = ''
    send_user = ''
    line_message = ''
    cleaned_message = ''
    linetype = 'message'
    
    print('processing', current_ds, 'at', fileLoc)
    log = codecs.open(fileLoc, 'r', encoding='utf-8', errors='ignore')
    soup = BeautifulSoup(log)

# the file looks like this:    
#<tr>
#<td class="datetime">
#<a name="l1285148000.0">
#<a href="#l1285148000.0">09:33</a>
#</a>
#</td>
#<td class="nickname">cdecker</td>
#<td>Well it&#39;s a project with 1&#39;500&#39;000 sloc
#</td>
#</tr>


    for trow in soup.find_all('tr'):
        linecounter += 1
        unix_time     = trow.td.a['name']
        time_of_entry = trow.td.a.next_sibling.get_text()
        send_user     = trow.td.next_sibling.next_sibling.get_text()
        line_message  = trow.td.next_sibling.next_sibling.next_sibling.next_sibling.get_text().rstrip()
        #print(date_of_entry,'|',linecounter,'|',unix_time,'|',time_of_entry,'|',send_user,'|',line_message,'|',linetype)

        #insert parsed data into databases
        try:
            cursor2.execute(u"INSERT INTO bitcoindev_irc(datasource_id, \
                line_num, \
                date_of_entry, \
                time_of_entry, \
                unix_time, \
                send_user, \
                line_message, \
                type)  \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                (current_ds, 
                linecounter,
                date_of_entry,
                time_of_entry,
                unix_time, 
                send_user, 
                line_message, 
                linetype
                ))
            db2.commit() 
        except pymysql.Error as error:
            print(error)
            db2.rollback()
                
        try:
            cursor3.execute(u"INSERT INTO bitcoindev_irc(datasource_id, \
                line_num, \
                date_of_entry, \
                time_of_entry, \
                unix_time, \
                send_user, \
                line_message, \
                type)  \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                (current_ds, 
                linecounter,
                date_of_entry,
                time_of_entry,
                unix_time, 
                send_user, 
                line_message, 
                linetype
                ))
            db3.commit() 
        except pymysql.Error as error:
            print(error)
            db3.rollback()

db1.close()
db2.close()
db3.close()  
cursor1.close()
cursor2.close() 
cursor3.close()       
