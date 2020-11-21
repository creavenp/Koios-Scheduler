from flask import Flask
from flask_apscheduler import APScheduler
import mysql.connector
from mysql.connector import errorcode
import time

app = Flask(__name__)
sched = APScheduler()

# This function will create the necessary cron Expression for the CronTrigger in the
# scheduler add_job functionality
def create_cronExp(scheduler):
    cronArr = ['*'] * 5
    scheduler_arr = scheduler.split('|')

    #if daily alert
    if scheduler_arr[0] == 'daily':

        #first position after daily
        if scheduler_arr[1] != '1':
            cronArr[2] = '*/' + str(scheduler_arr[1])

        #hour
        cronArr[1] = str(scheduler_arr[2])

        #minute value
        cronArr[0] = str(scheduler_arr[3])

    #if weekly alert
    elif scheduler_arr[0] == 'weekly':

        #weeks value
        if scheduler_arr[1] != '1':
            cronArr[2] = '*/' + str(scheduler_arr[1] * 7)

        #comma_separated_days
        comma_days = str(scheduler_arr[2])
        arr = comma_days.split(',')
        temp = list()
        string = ''
        for i,num in enumerate(arr):
            num = int(num)
            num = str(num - 1)
            if i == (len(arr) - 1):
                string += num
            else:
                string += num + ','
        cronArr[4] = string

        #hour
        cronArr[1] = str(scheduler_arr[3])

        #minute
        cronArr[0] = str(scheduler_arr[4])

    return cronArr #(cronArr[0] + ' ' + cronArr[1] + ' ' + cronArr[2] + ' ' + cronArr[3] + ' ' + cronArr[4])

# This function is designed to call the flask python server that will send the push notifications
def post_func():
    #this will call that server (have to implement the actual call to the other flask
    # server in here though). At the moment this serves as a check that will log to the
    # screen to make it sure it is functioning correctly
    print('Made it')

# The main function of this script that will go into the database, extract the data, and then create the
# scheduler from it
def KoiosSchedulerFunction(scheduler_dict):
        # Connect to MySQL database
        try:
            cnx = mysql.connector.connect(user='test', password='test',
                                          host='127.0.0.1', database='mcs')
        except mysql.connector.Error as err:
          if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
          elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
          else:
            print(err)

        cursor = cnx.cursor()

        # Collect the DB data: create a dictionary that uses study_id as key
        scheduler_dict = dict()

        #while 1:
        query = ("SELECT schedule_id, study_id, title, message, is_active, scheduler, sync_flag  FROM schedule_notification")
        cursor.execute(query)
        for (schedule_id, study_id, title, message, is_active, scheduler, sync_flag) in cursor:
            schedule_id = str(schedule_id)
            key = schedule_id
            # Determine if a cron job should be created
            if key not in scheduler_dict.keys() and is_active == 1:
                cronArr = create_cronExp(str(scheduler))
                scheduler_dict[schedule_id] = [study_id, title, message, is_active, scheduler, sync_flag]
                sched.add_job(id = schedule_id, func = post_func, trigger = 'cron', minute = cronArr[0], hour = cronArr[1], day = cronArr[2], day_of_week = cronArr[4])
            if key in scheduler_dict.keys() and is_active == 1 and sync_flag == 1:
                # initially remove the scheduled job and delete the data from the hash map
                sched.remove_job(schedule_id)
                del scheduler_dict[schedule_id]
                # now recreate the cronArr and re-add the job with the update information
                cronArr = create_cronExp(str(scheduler))
                schedule_id = str(schedule_id)
                scheduler_dict[schedule_id] = [study_id, title, message, is_active, scheduler, sync_flag]
                sched.add_job(id = schedule_id, replace_existing = True, func = post_func, trigger ='cron', minute = cronArr[0], hour = cronArr[1], day = cronArr[2], day_of_week = cronArr[4])
                # update the sync_flag back to 0 in the Database
                update_query = ("UPDATE schedule_notification SET sync_flag = 0 WHERE schedule_id = %s")
                val = (schedule_id)
                cursor.execute(update_query, val)
                cnx.commit()
            # Determine if a cron job should be removed
            if key in scheduler_dict.keys() and is_active == 0 or None:
                sched.remove_job(schedule_id)
                del scheduler_dict[schedule_id]

        # close off the mySQL cursor to be utilized when the function is called again
        cursor.close()
        cnx.close()



if __name__ == '__main__':

    # This scheduler will make the larger necessary scheduler run at specific intervals, so as to not overload the system with constant
    # checks on the database

    # At the moment it will check the database at the start of every hour every day for updates and call the flaskFunction
    scheduler_dict = dict()
    sched.add_job(id = 'main', func = lambda: KoiosSchedulerFunction(scheduler_dict), trigger = 'cron', minute = '0', hour = '*', day='*', month='*')
    sched.start()

    # can change the port to any value
    app.run(host = '0.0.0.0', port = 8080)
