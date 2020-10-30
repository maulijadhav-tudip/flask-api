"""Database interface for skills-db."""

import os
import pymysql
from flask import jsonify

db_user = os.environ.get('CLOUD_SQL_USERNAME')
db_password = os.environ.get('CLOUD_SQL_PASSWORD')
db_name = os.environ.get('CLOUD_SQL_DATABASE_NAME')
db_connection_name = os.environ.get('CLOUD_SQL_CONNECTION_NAME')


def open_connection():
    """Open database connection."""
    unix_socket = '/cloudsql/{}'.format(db_connection_name)
    try:
        if os.environ.get('GAE_ENV') == 'standard':
            conn = pymysql.connect(
                user=db_user,
                password=db_password,
                unix_socket=unix_socket,
                db=db_name,
                cursorclass=pymysql.cursors.DictCursor)
    except pymysql.MySQLError as e:
        print(e)
    return conn


def check_login(email_id):
    """Check user existence in database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = "SELECT * FROM Users WHERE email_id=%s;"
        val = (email_id)
        rst = cursor.execute(query, val)
        if rst > 0:
            return True
        else:
            name = email_id.split("@")[0]
            role = 'CE'
            organization = 'Google Cloud'
            query1 = "INSERT INTO Users(email_id,name,role,organization) VALUES (%s ,%s,%s,%s);"
            val = (email_id, name, role, organization)
            result = cursor.execute(query1, val)
            conn.commit()
            query2 = "INSERT INTO UserSkillsPrioDB (user_id, skill_id) SELECT Users.user_id, Taxonomy.skill_id FROM Users INNER JOIN Taxonomy ON Users.role=Taxonomy.personality_type WHERE Users.email_id=%s"
            val1 = (email_id)
            result1 = cursor.execute(query2, val1)
            conn.commit()
            return True
        conn.close()


def get_skills(email):
    """Retrieve skills from database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        result = cursor.execute('SELECT Taxonomy.skill_hierarchy_level_1,Taxonomy.skill_hierarchy_level_2,Taxonomy.skill_hierarchy_level_3, Users.user_id,skill_name,Taxonomy.skill_id,Taxonomy.skill_type,Taxonomy.skill_description_level_0,Taxonomy.skill_description_level_1,Taxonomy.skill_description_level_2,Taxonomy.skill_description_level_3,Taxonomy.skill_description_level_4, Taxonomy.skill_ranking_level_0, Taxonomy.skill_ranking_level_1, Taxonomy.skill_ranking_level_2, Taxonomy.skill_ranking_level_3, Taxonomy.skill_ranking_level_4 FROM UserSkillsPrioDB INNER JOIN Users ON  UserSkillsPrioDB.user_id = Users.user_id INNER JOIN Taxonomy ON UserSkillsPrioDB.skill_id = Taxonomy.skill_id WHERE email_id = %s order by priority_rating, skill_id,UserSkillsPrioDB.updated_at IS NULL DESC;', email)
        skills = cursor.fetchall()
        if result > 0:
            got_skills = jsonify(skills)
        else:
            got_skills = 'No skills in DB'
    conn.close()
    return got_skills


def save_rating(skill_id,user_id,rating):
    """Persist rating to database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = "INSERT INTO UsersSkillDB(user_id,skill_id,skill_rating) VALUES((SELECT user_id FROM Users WHERE user_id = %s),(SELECT skill_id FROM Taxonomy WHERE skill_id = %s),%s)"
        val = (user_id, skill_id, rating)
        result = cursor.execute(query, val)
        conn.commit()
    conn.close()


def get_dashboard(email):
    """Retrieve dashboard data from database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        result = cursor.execute('SELECT Users.name,Taxonomy.skill_name,Taxonomy.skill_id,skill_rating FROM UsersSkillDB INNER JOIN Users ON  UsersSkillDB.user_id = Users.user_id INNER JOIN Taxonomy ON UsersSkillDB.skill_id = Taxonomy.skill_id WHERE email_id = %s ;',email)
        skills = cursor.fetchall()
        if result > 0:
            got_dashboard = jsonify(skills)
        else:
            got_dashboard = 'No self-attested data available in DB'
    conn.close()
    return got_dashboard


def set_skill_priority(user_id, skill_id):
    """Persist skill priority to database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = "UPDATE UserSkillsPrioDB SET priority_rating=30, updated_at=NOW() WHERE  user_id = %s AND skill_id = %s ;"
        val = (user_id, skill_id)
        result = cursor.execute(query, val)
        conn.commit()
    conn.close()


def get_rated_skill_count(user_id):
    """Retrieve count of rated skills, unrated skill and expired skill from database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = 'SELECT (SELECT COUNT(skill_id) FROM UserSkillsPrioDB WHERE updated_at IS NULL AND user_id = %s) AS unrated_skill_count,(SELECT COUNT(skill_id) FROM UserSkillsPrioDB WHERE updated_at IS NOT NULL AND priority_rating >= 1 AND user_id = %s) AS rated_skill_count,(SELECT COUNT(skill_id) FROM UserSkillsPrioDB WHERE updated_at IS NOT NULL AND priority_rating < 1 AND user_id = %s) AS expired_skill_count'
        val = (user_id, user_id, user_id)
        result = cursor.execute(query, val)
        skills_count = cursor.fetchall()
        if result > 0:
            got_skills_count = jsonify(skills_count)
        else:
            got_skills_count = 'No self-attested data available in DB'
    conn.close()
    return got_skills_count


def set_last_trigger_time(user_id, timestamp):
    """Persist last trigger time to database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = "INSERT INTO Survey(user_id,ext_triggered, ext_closed, ext_postponed, postpone_option) VALUES ((SELECT user_id FROM Users WHERE user_id = %s),%s,null,null,null);"
        val = (user_id, timestamp)

        result = cursor.execute(query, val)
        conn.commit()
    conn.close()


def log_extension_closed_time(user_id, timestamp):
    """Persist extension closed time to database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = "UPDATE Survey SET ext_closed = %s WHERE user_id = (SELECT user_id FROM (SELECT * FROM Survey WHERE user_id = %s ORDER BY ext_triggered DESC limit 1) as Survey_new) AND ext_triggered = (SELECT ext_triggered FROM (SELECT * FROM Survey WHERE user_id = %s ORDER BY ext_triggered DESC limit 1) as Survey_new);"
        val = (timestamp, user_id, user_id)
        result = cursor.execute(query, val)
        conn.commit()
    conn.close()


def log_extension_postponed_time(user_id, timestamp, postpone_option):
    """Persist extension postponed time to database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = "UPDATE Survey SET ext_postponed = %s ,postpone_option = %s WHERE user_id = (SELECT user_id FROM (SELECT * FROM Survey WHERE user_id = %s ORDER BY ext_triggered DESC limit 1) as Survey_new) AND ext_triggered = (SELECT ext_triggered FROM (SELECT * FROM Survey WHERE user_id = %s ORDER BY ext_triggered DESC limit 1) as Survey_new);"
        val = (timestamp, postpone_option, user_id, user_id)
        result = cursor.execute(query, val)
        conn.commit()
    conn.close()


def get_survey_data(email):
    """Retrieve survey data from database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = 'SELECT Users.user_id, Survey.ext_triggered, Survey.ext_closed, Survey.ext_postponed FROM Survey INNER JOIN Users ON Survey.user_id = Users.user_id WHERE email_id = %s ORDER BY ext_triggered DESC LIMIT 1;'
        result = cursor.execute(query, email)
        skills = cursor.fetchall()
        if result > 0:
            survey_data = jsonify(skills)
        else:
            survey_data = 'No skills in DB'
    conn.close()
    return survey_data


def update_prompting():
    """Update priority rating in database."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = "UPDATE UserSkillsPrioDB SET priority_rating=30 - DATEDIFF(now(), updated_at)  WHERE updated_at IS NOT NULL;"
        result = cursor.execute(query)
        conn.commit()
    conn.close()


def get_triggered_count_per_day(email):
    """Get number of times the popup is triggered."""
    conn = open_connection()
    with conn.cursor() as cursor:
        query = 'SELECT COUNT(Survey.user_id) AS trigger_count FROM Survey INNER JOIN Users ON Users.user_id = Survey.user_id WHERE Users.email_id = %s;'
        result = cursor.execute(query, email)
        count = cursor.fetchall()
        trigger_count = jsonify(count)
    conn.close()
    return trigger_count


def delete_todays_skills_data(user_id):
    """Delete Today's user data without the survey logs"""
    conn = open_connection()
    with conn.cursor() as cursor:
        query1 = 'DELETE FROM UsersSkillDB WHERE user_id=%s AND date(rated_at) = CURDATE()'
        val1 = (user_id)
        result1 = cursor.execute(query1, val1)
        query2 = 'UPDATE UserSkillsPrioDB SET priority_rating=NULL, updated_at=NULL where  user_id = %s and date(updated_at) = CURDATE()'
        val2 = (user_id)
        result2 = cursor.execute(query2, val2)
        conn.commit()
    conn.close()
    return 'Data deleted for today'


def delete_all_user_data_with_logs(user_id):
    """Delete all the user data including the survey logs"""
    conn = open_connection()
    with conn.cursor() as cursor:
        query1 = 'DELETE FROM UsersSkillDB WHERE user_id=%s'
        val1 = (user_id)
        result1 = cursor.execute(query1, val1)
        query2 = 'DELETE FROM Survey WHERE user_id=%s'
        val2 = (user_id)
        result2 = cursor.execute(query2, val2)
        query3 = 'UPDATE UserSkillsPrioDB SET priority_rating=NULL, updated_at=NULL WHERE  user_id = %s'
        val3 = (user_id)
        result3 = cursor.execute(query3, val3)
        conn.commit()
    conn.close()
    return 'All data deleted successfully with interaction logs.'


def delete_all_user_data_without_logs(user_id):
    """Delete all the user data without the survey logs"""
    conn = open_connection()
    with conn.cursor() as cursor:
        query1 = 'DELETE FROM UsersSkillDB WHERE user_id=%s'
        val1 = (user_id)
        result1 = cursor.execute(query1, val1)
        query2 = 'UPDATE UserSkillsPrioDB SET priority_rating=NULL, updated_at=NULL WHERE  user_id = %s'
        val2 = (user_id)
        result2 = cursor.execute(query2, val2)
        conn.commit()
    conn.close()
    return 'Data deleted successfully'

