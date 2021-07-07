import mysql.connector
from config import USER, PASSWORD, HOST
#go into the config gile and give me the host, user and password. 

#defines the connection class we use later for the exception.
class DbConnectionError(Exception):
    pass

#variable is called connection. uses connect through mysql.
# .connect is an inbuilt function.
#details what you need to access it - host, user, password etc.
#arguments you need.
#database name - db_name.
#you could also put the config details here, but it's bad practise.
#you don't want your deets flying around if you upload it.
#auth plug in - just telling git what sort of encryption to use.
#db_name defined in a different function, or you can define in the function in this case.
#we have now created our connection to the database. returns connection.
#run it - you have an sql connector object if succesful.

def _connect_to_db(db_name):
    cnx = mysql.connector.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        auth_plugin='mysql_native_password',
        database=db_name
    )
    return cnx

#cursor is essentially where we store the query.

# EXAMPLE 1
#a cursor is something you can move along. cur - cursor is what that means.
#specify the db name, and then the connection you're using (which is the previous
# function). 
#query is from SQL - that's exactly what's happening here. 
# what query do you want? In our case, we want everything fro abc report.
#we want to put it in a string, and since more llines than one, we use three ".
#cur.execute(query) is how we actually move forward. We execute query in cursor.
#iF YOU PRINT the cursor, it will tell you that the query is now set up to activate.
#we just need to call it. 
#how to call - result. fetchall is an inbuilt function .
# you can make it easier to read with a for loop. it's not necessary, but makes it prettier.
# then it iterates through a list. prints each thing one by one. 
#then it closes the connection - you must ALWAYS close the connection to the database.
#similar to files - when you open, you always need to close them.
# will always have db connection, always have the connector. create the db connection, cursor and query. 
#will always have the db_connect to the result line.

#try and except block to catch errors, such as you input the wrong database name or miss off a letter.
#or there's a general connection error. We have the wrong connection details. Password etc.
#finally code will be executed every time. 
#if we have a database connection that is open, we're going to close it and then print out a message.

def get_all_records():
    try:
        db_name = 'tests'  # update as required
        db_connection = _connect_to_db(db_name)
        cur = db_connection.cursor()
        print("Connected to DB: %s" % db_name)

        query = """SELECT * FROM abcreport"""
        cur.execute(query)
        result = cur.fetchall()  # this is a list with db records where each record is a tuple

        for i in result:
            print(i)
        cur.close()

    except Exception:
        raise DbConnectionError("Failed to read data from DB")

    finally:
        if db_connection:
            db_connection.close()
            print("DB connection is closed")


# EXAMPLE 2
# so what it accepts is sold items from the table, and the percentage commission.
# create a new list where we'll store all of the data.
# go through each row append the third column.
# sum up all the items in sales then get the percentage out of them.

def calc_commission(sold_items, commission):
    sales = []

    for item in sold_items:
        sales.append(item[2])

    commission = sum(sales) * (commission / 100)
    return commission

#calc_commission result to return

#want this function to isolate the representatives in the table.
#the same structure. Different query - selects all information regarding reps.
#where the representative is equal to something. format with the rep_name. you can run it in 
#workbench to see what you're expecting.
#what we're missing is rep_name, we haven't passed it in the function.
#in def main, then get_all_records_for_rep 'Morgan'
#rep_name is the parameter used.

#can we do calculations on a database? yes WE CAN.
#this is what line 120 does. We have compensation. Round the number to two decimal places.
#commission based on the result and the commission percentage.

def get_all_records_for_rep(rep_name):
    try:
        db_name = 'tests'
        db_connection = _connect_to_db(db_name)
        cur = db_connection.cursor()
        print("Connected to DB: %s" % db_name)

        query = """SELECT Item, Units, Total FROM abcreport WHERE Rep = '{}'""".format(
            rep_name)  # note extra speechmarks around the curly brakets -- we need them!
        cur.execute(query)
        result = cur.fetchall()  # this is a list with db records where each record is a tuple

        for i in result:
            print(i)

        cur.close()

        comp = round(calc_commission(result, commission=10), 2)

    except Exception:
        raise DbConnectionError("Failed to read data from DB")

    finally:
        if db_connection:
            db_connection.close()
            print("DB connection is closed")

    print("Commission for {} is £{}".format(rep_name, comp))
    return rep_name, comp


# EXAMPLE 3 - INSERT INTO TABLE

import datetime as dt
#just importing datetime, this can go at the top.

record = {
    'OrderDate': '2020-12-15',
    'Region': 'Central',
    'Rep': 'Johnson',
    'Item': 'post-it-notes',
    'Units': 220,
    'UnitCost': 2.5,
    'Total': 220 * 2.5,
}

#we've gotten the records and also filtered them.
#now we want to add a new one.
#put into main if you want to run. The record is above and follows the table structure.
#all in a try, except, finally block. Only thing that's different is the query.
#remember to indent properly.
#where are you going to join them - keys, as they are the same. 
#from record, join them on order date, region etc. 
# Placeholders on each order. Use format to replace everything.
#each of these values corresponds.

#run it, and see if we get an error. Integrity error. Duplicate entry.
#It's already in the table, therefore we can't add it again.
#primary key is the same. Instead of post-it-notes, we could say binder.
#it returned no error, but has not updated the table.
#you have to commit, otherwise it won't get changed. Always check the change has gone through. 
#the first curly brackets come for the format.

def insert_new_record(record):
    try:
        db_name = 'tests'
        db_connection = _connect_to_db(db_name)
        cur = db_connection.cursor()
        print("Connected to DB: %s" % db_name)

        query = """INSERT INTO abcreport ({}) VALUES ('{}', '{}', '{}', '{}', {}, {}, {})""".format(
            ', '.join(record.keys()),
            record['OrderDate'],
            record['Region'],
            record['Rep'],
            record['Item'],
            record['Units'],
            record['UnitCost'],
            record['Total'],
        )
        cur.execute(query)
        db_connection.commit()  # VERY IMPORTANT, otherwise, rows would not be added or reflected in the DB!
        cur.close()

    except Exception:
        raise DbConnectionError("Failed to read data from DB")

    finally:
        if db_connection:
            db_connection.close()
            print("DB connection is closed")

    print("Record added to DB")

#you're just calling the function in the main body.Or you could just copy the function and put it in main, instead of having
#it as a separate function. it just does the same thing if you call it in main.

def main():
    get_all_records()
    # get_all_records_for_rep('Morgan')
    #insert_new_record(record)


if __name__ == '__main__':
    main()
