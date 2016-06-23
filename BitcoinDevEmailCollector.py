# -*- coding: utf-8 -*-
# This program is free software; you can redistribute it
# and/or modify it under the same terms as Python itself.
#
# Copyright (C) 2004-2016 Evan Ashwell
# With code contributions from:
# Megan Squire
#
# We're working on this at http://flossmole.org - Come help us build
# an open and accessible repository for data and analyses for open
# source projects.
#
# If you use this code or data for preparing an academic paper please
# provide a citation to:
#
# Howison, J., Conklin, M., & Crowston, K. (2006). FLOSSmole:
# A collaborative repository for FLOSS research data and analyses.
# International Journal of Information Technology and Web Engineering,
# 1(3), 17–26.
#
# and
#
# FLOSSmole (2004-2016) FLOSSmole: a project to provide academic access to data
# and analyses of open source projects.  Available at http://flossmole.org
################################################################
# usage:
# > python BitcoinDevEmailCollector.py <new_datasource_id> <date-to-start> <pw>
#
# THIS DATASOURCE IS THE NEXT ONE AVAIL IN THE DB - AND IT WILL GET INCREMENTED
# DATE TO START is the oldest un-collected date;
# the script will go through all months available
# example usage:
# > python BitcoinDevEmailCollector.py 61260 20110601
#
# purpose:
# grab all the Dev emails from
# https://lists.linuxfoundation.org/pipermail/bitcoin-dev
################################################################
import sys
import pymysql
try:
    import urllib.request as urllib2
except ImportError:
    import urllib2
import codecs
import os
from datetime import datetime
from bs4 import BeautifulSoup
import calendar
import re
import html
import dateutil.relativedelta

# takes user inputs
# datasource_id standa for the next available data source
# the start_date is the day after the last known date data was collected
datasource_id = str(sys.argv[1])
start_date = str(sys.argv[2])
password = str(sys.argv[3])
currDate = datetime.now()
newDS = datasource_id

if datasource_id and start_date:
    # ======
    # LOCAL
    # ======
    try:
        dbh2 = pymysql.connect(host='grid6.cs.elon.edu',
                               database='bitcoin',
                               user='megan',
                               password=password,
                               charset='utf8')
    except pymysql.Error as err:
        print(err)
    cursor2 = dbh2.cursor()
    
    # =======
    # REMOTE
    # =======
    try:
        dbh3 = pymysql.connect(host='flossdata.syr.edu',
                               database='bitcoin',
                               user='megan',
                               password=password,
                               charset='utf8')
    except pymysql.Error as err:
        print(err)
    cursor3 = dbh3.cursor()
    
    insertQuery = "INSERT INTO `bitcoindev_email`\
                (`datasource_id`,\
                `header`,\
                `sender`,\
                `email`,\
                `text`,\
                `url`,\
                `file_location`,\
                `date_of_entry`,\
                `last_updated`)\
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    start_date = datetime(int(start_date[0:4]), 
                        int(start_date[5:6]),
                        int(start_date[6:]))
    print(start_date)

    urlstem = "https://lists.linuxfoundation.org/pipermail/bitcoin-dev/"
    os.mkdir("htmls")
    os.mkdir(datasource_id)

    num = 0
    while start_date <= currDate:
        year = start_date.year
        months = start_date.month
        month = calendar.month_name[months]

        newURL = urlstem + str(year) + "-" + month + "/date.html"

        try:
            html1 = urllib2.urlopen(newURL).read()
        except urllib2.HTTPError as error:
            print(error)
        else:
            fileloc = "htmls/" + str(year) + "-" + month
            outfile = codecs.open(fileloc, 'w')
            outfile.write(str(html1))
            outfile.close()

        soup = BeautifulSoup(open(fileloc), "lxml")

        for link in soup.find_all('a'):
            linker = str(link.get('href'))
            formatting = re.search('(\d\d\d\d\d\d)', linker)
            if formatting:
                secondURL = (urlstem + str(year) + "-" + month + "/" +
                             formatting.group(1) + ".html")

                try:
                    html2 = urllib2.urlopen(secondURL).read()
                except urllib2.HTTPError as error:
                    print(error)
                else:
                    fileloc = (datasource_id + "/" + formatting.group(1) +
                               ".txt")
                    outfile = codecs.open(fileloc, 'w')
                    outfile.write(str(html2))
                    outfile.close()
                    num = num + 1

                    try:
                        log = (codecs.open(fileloc, 'r', encoding='utf-8',
                                           errors='ignore'))
                        line = log.read()
                        line = line[2:]
                        line = line[:-1]
                        log = line.split("\\n")
                    except pymysql.Error as err:
                        print(err)

                    for line in log:
                        title = re.search('TITLE="(.*?)">(.*)', line)
                        if title:
                            header = title.group(1)
                            userEmail = title.group(2)
                            print(header)

                            emailFinder = re.search('(.*?) at (.*)', userEmail)
                            if emailFinder:
                                email = (emailFinder.group(1) + "@" +
                                         emailFinder.group(2))
                                print(email)
                        message = re.search("(Messages sorted by:)", line)
                        if not message:
                            name = re.search("\<B\>(.*?)\<", line)
                            if name:
                                sender = name.group(1)
                                print(num, ": " + sender)
                        date = re.search('<I>(.*?)' + str(year) + '</I>', line)
                        if date:
                            entryDate = date.group(1)
                            entryDate = entryDate + str(year)
                            entryDate = (datetime.strptime(entryDate,
                                         "%a %b %d %H:%M:%S UTC  %Y"))
                            print(entryDate)

                    soup = BeautifulSoup(open(fileloc), "lxml")
                    text = soup.find('pre')
                    text = str(text)
                    text = text[5:]
                    text = text[:-6]
                    text = text.split("\\n")

                    fullText = ""
                    for txt in text:
                        fullText = fullText + "\n" + html.unescape(txt)

                    # ======
                    # LOCAL
                    # ======
                    try:
                        cursor2.execute(insertQuery, (newDS, header, sender,
                                                      email, fullText,
                                                      secondURL, fileloc,
                                                      entryDate, currDate))
                        dbh2.commit()
                    except pymysql.Error as error:
                        print(error)
                        dbh2.rollback()

                    # =======
                    # REMOTE
                    # =======
                    try:
                        cursor3.execute(insertQuery, (newDS, header, sender,
                                                       email, fullText,
                                                       secondURL, fileloc,
                                                       entryDate, currDate))
                        dbh3.commit()
                    except pymysql.Error as error:
                        print(error)
                        dbh3.rollback()

                    newDS = int(newDS) + 1
                    start_date = (start_date +
                      dateutil.relativedelta.relativedelta(months=1))

else:
    print("You need both a datasource_id and a date to start on\
    your commandline.")
    exit

cursor2.close()
cursor3.close()

dbh2.close()
dbh3.close()
