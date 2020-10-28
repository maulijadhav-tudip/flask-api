"""Main file for Skills-db backend."""

from db import get_dashboard
from db import get_progress_count
from db import get_skills
from db import get_survey_data
from db import log_extension_closed_time
from db import log_extension_postponed_time
from db import save_rating
from db import set_last_trigger_time
from db import set_skill_priority
from db import update_prompting
from db import check_login
from flask import request
from flask import Flask
from flask import jsonify

app = Flask(__name__)


@app.route('/get-skills', methods=['GET'])
def skills():
    """Endpoint to check user information in database."""
    if request.args:
        email = request.args.get('email')
        check_login(email)
        return get_skills(email)
    else:
        error_msg = 'Invalid parameters'
        return error_msg


@app.route('/rate-skill', methods=['POST'])
def rate_skill():
    """Endpoint to persist skill rating to database."""
    if request.method == 'POST':
        if request.form:
            skill_id = request.form.get("skillId")
            user_id = request.form.get("userId")
            skill_rating = request.form.get("rating")

            save_rating(skill_id, user_id, skill_rating)
            return 'Skill rating saved successfully.'
        else:
            return jsonify({"msg": "Missing form data in request"}), 400

    else:
        error_msg = 'Invalid method'
        return error_msg


@app.route('/get-dashboard', methods=['GET'])
def dashboard():
    """Endpoint to retrieve dashboard data from database."""
    if request.args:
        email = request.args.get('email')
        return get_dashboard(email)
    else:
        error_msg = 'Invalid parameters'
        return error_msg


@app.route('/update-skill-priority', methods=['POST'])
def update_skill_priority():
    """Endpoint to update skill priority in database."""
    if request.method == 'POST':
        if request.form:
            skill_id = request.form.get("skillId")
            user_id = request.form.get("userId")

            set_skill_priority(user_id, skill_id)
            return 'Skill priority updated successfully.'
        else:
            return jsonify({"msg": "Missing form data in request"}), 400

    else:
        error_msg = 'Invalid method'
        return error_msg


@app.route('/get-progress-count', methods=['GET'])
def progress_skill_count():
    """Endpoint to retrieve count of rated skill, unrated skill and expired skill from database."""
    if request.args:
        user_id = request.args.get('userId')
        skills_count = get_progress_count(user_id)
    else:
        skills_count = 'Invalid parameters'
    return skills_count


@app.route('/save-trigger-time', methods=['POST'])
def save_trigger_time_and_count():
    """Endpoint to persist trigger time and count to database."""
    if request.method == 'POST':
        if request.form:
            timestamp = request.form.get("timestamp")
            user_id = request.form.get("userId")

            set_last_trigger_time(user_id, timestamp)
            return 'Extension trigger event logged successfully.'
        else:
            return jsonify({"msg": "Missing form data in request"}), 400
    else:
        error_msg = 'Invalid method'
        return error_msg


@app.route('/log-extension-closed-time', methods=['POST'])
def save_closed_time():
    """Endpoint to persist closed time to database."""
    if request.method == 'POST':
        if request.form:
            timestamp = request.form.get("timestamp")
            user_id = request.form.get("userId")

            log_extension_closed_time(user_id, timestamp)
            return 'Extension close event logged successfully.'
        else:
            return jsonify({"msg": "Missing form data in request"}), 400
    else:
        error_msg = 'Invalid method'
        return error_msg


@app.route('/log-extension-postponed-time', methods=['POST'])
def save_postponed_time():
    """Endpoint to persist postponed time to database."""
    if request.method == 'POST':
        if request.form:
            timestamp = request.form.get("timestamp")
            user_id = request.form.get("userId")
            postpone_option = request.form.get("postponeOption")

            log_extension_postponed_time(user_id, timestamp, postpone_option)
            return 'Extension postpone event logged successfully.'
        else:
            return jsonify({"msg": "Missing form data in request"}), 400
    else:
        error_msg = 'Invalid method'
        return error_msg


@app.route('/get-survey-data', methods=['GET'])
def survey_data():
    """Endpoint to retrieve survey data from database."""
    if request.args:
        email = request.args.get('email')
        return get_survey_data(email)
    else:
        error_msg = 'Invalid parameters'
        return error_msg


@app.route('/triggered-count-per-day', methods=['GET'])
def triggered_count():
    """Endpoint to retrieve triggered count from database."""
    if request.args:
        email = request.args.get('email')
        return get_triggered_count_per_day(email)
    else:
        error_msg = 'Invalid parameters'
        return error_msg


@app.route('/set-skill-priority', methods=['GET'])
def update_skill_priority_per_day():
    """Endpoint to update skill priority."""
    return update_prompting()


@app.route('/delete-user-data', methods=['GET'])
def delete_history():
    """Endpoint to delete user related data from database."""
    if request.args:
        user_id = request.args.get('userId')
        return delete_user_data(user_id)
    else:
        error_msg = 'Invalid parameters'
        return error_msg


if __name__ == '__main__':
    app.run()

