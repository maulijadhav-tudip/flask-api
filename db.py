#db.py
import os
import pymysql
from flask import jsonify

db_user = os.environ.get('CLOUD_SQL_USERNAME')
db_password = os.environ.get('CLOUD_SQL_PASSWORD')
db_name = os.environ.get('CLOUD_SQL_DATABASE_NAME')
db_connection_name = os.environ.get('CLOUD_SQL_CONNECTION_NAME')


def open_connection():
    unix_socket = '/cloudsql/{}'.format(db_connection_name)
    try:
        if os.environ.get('GAE_ENV') == 'standard':
            conn = pymysql.connect(user=db_user, password=db_password,
                                unix_socket=unix_socket, db=db_name,
                                cursorclass=pymysql.cursors.DictCursor
                                )
    except pymysql.MySQLError as e:
        print(e)

    return conn


def get_skills(email):
    conn = open_connection()
    with conn.cursor() as cursor:
        result = cursor.execute('select Users.user_id,skill_name,Taxonomy.skill_id,Taxonomy.skill_description_level_0,Taxonomy.skill_description_level_1,Taxonomy.skill_description_level_2,Taxonomy.skill_description_level_3,Taxonomy.skill_description_level_4 from UserSkillsPrioDB INNER JOIN Users on  UserSkillsPrioDB.user_id = Users.user_id inner join Taxonomy on UserSkillsPrioDB.skill_id = Taxonomy.skill_id where email_id = %s order by priority_rating;', email)
        skills = cursor.fetchall()
        if result > 0:
            got_skills = jsonify(skills)
        else:
            got_skills = 'No skills in DB'
    conn.close()
    return got_skills


def save_rating(skill_id,user_id,rating):
    conn = open_connection()
    with conn.cursor() as cursor:
        query = "select * from UsersSkillDB where user_id=%s and skill_id=%s;"
        val = (user_id,skill_id)
        rst = cursor.execute(query,val)
        if rst > 0:
            sql = "update UsersSkillDB set skill_rating  =%s where user_id=%s and skill_id=%s;"
            val = (rating,user_id,skill_id)

        else:
            sql = "insert into UsersSkillDB(user_id,skill_id,skill_rating) values ((select user_id from Users where user_id=%s),(select skill_id from Taxonomy where skill_id=%s),%s);"
            val = (user_id,skill_id,rating)

        result = cursor.execute(sql,val)
        conn.commit()
    conn.close()


def get_dashboard(email):
    conn = open_connection()
    with conn.cursor() as cursor:
        result = cursor.execute('select Users.user_name,Taxonomy.skill_name,Taxonomy.skill_id,skill_rating from UsersSkillDB INNER JOIN Users on  UsersSkillDB.user_id = Users.user_id inner join Taxonomy on UsersSkillDB.skill_id = Taxonomy.skill_id where email_id = %s ;',email)
        skills = cursor.fetchall()
        if result > 0:
            got_skills = jsonify(skills)
        else:
            got_skills = 'No self-attested data available in DB'
    conn.close()
    return got_skills


def set_skill_priority(user_id, skill_id):
    conn = open_connection()
    with conn.cursor() as cursor:
        sql = "update UserSkillsPrioDB set priority_rating = ((select count(skill_id) from (select * from UserSkillsPrioDB where user_id = %s) as UserSkillsPrioDB_new1) + (select priority_rating from (select * from UserSkillsPrioDB where user_id = %s and skill_id = %s ) as UserSkillsPrioDB_new2)) where  user_id = %s and skill_id = %s ;"
        val = (user_id, user_id, skill_id, user_id, skill_id)
        result = cursor.execute(sql, val)
        conn.commit()
    conn.close()


def get_self_attest_count_per_day(user_id):
    conn = open_connection()
    with conn.cursor() as cursor:
        result = cursor.execute('SELECT COUNT(skill_id) FROM UsersSkillDB where date(created_at)=current_date() and user_id = %s;', user_id)
        skills = cursor.fetchall()
        if result > 0:
            got_skills = jsonify(skills)
        else:
            got_skills = 'No self-attested data available in DB'
    conn.close()
    return got_skills


def set_last_trigger_time(user_id, timestamp):
    conn = open_connection()
    with conn.cursor() as cursor:
        rst = cursor.execute("select * from Survey where user_id=%s", user_id)
        if rst > 0:
            sql = "update Survey set trigger_count  = ((select trigger_count from (select * from Survey where user_id = %s) as Survey_new) + 1), ext_triggered = %s where  user_id = %s;"
            val = (user_id, timestamp, user_id)

        else:
            sql = "insert into Survey values (%s,%s,null,null,1);"
            val = (user_id, timestamp)

        result = cursor.execute(sql,val)
        conn.commit()
    conn.close()


def log_extension_closed_time(user_id, timestamp):
    conn = open_connection()
    with conn.cursor() as cursor:
        sql = "update Survey set ext_closed = %s where  user_id = %s;"
        val = (timestamp, user_id)
        result = cursor.execute(sql,val)
        conn.commit()
    conn.close()


def log_extension_postponed_time(user_id, timestamp):
    conn = open_connection()
    with conn.cursor() as cursor:
        sql = "update Survey set ext_postponed = %s where  user_id = %s;"
        val = (timestamp, user_id)
        result = cursor.execute(sql, val)
        conn.commit()
    conn.close()


def get_survey_data(user_id):
    conn = open_connection()
    with conn.cursor() as cursor:
        result = cursor.execute('select * from Survey where user_id = %s ;', user_id)
        skills = cursor.fetchall()
        if result > 0:
            got_skills = jsonify(skills)
        else:
            got_skills = 'No self-attested data available in DB'
    conn.close()
    return got_skills
