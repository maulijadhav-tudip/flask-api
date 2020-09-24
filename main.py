from flask import Flask, jsonify, request
from db import get_skills, save_rating, get_dashboard, set_skill_priority, get_self_attest_count_per_day, set_last_trigger_time

app = Flask(__name__)


@app.route('/get-skills', methods=['GET'])
def skills():

    if request.args:
        email = request.args.get('email')
        return get_skills(email)
        return email
    else:
        got_skills = 'Invalid parameters'
        return got_skills


@app.route('/rate-skill', methods=['POST'])
def rate_skill():
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
    if request.args:
        email = request.args.get('email')
        return get_dashboard(email)
    else:
        got_skills = 'Invalid parameters'
        return got_skills


@app.route('/update-skill-priority', methods=['POST'])
def update_skill_priority():
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


@app.route('/get-self-attest-count-per-day', methods=['GET'])
def self_attest_count():
    if request.args:
        user_id = request.args.get('userId')
        return get_self_attest_count_per_day(user_id)
    else:
        got_skills = 'Invalid parameters'
        return got_skills


@app.route('/save-trigger-time', methods=['POST'])
def save_trigger_time_and_count():
    if request.method == 'POST':
        if request.form:
            timestamp = request.form.get("timestamp")
            user_id = request.form.get("userId")

            set_last_trigger_time(user_id, timestamp)
            return 'Skill priority updated successfully.'
        else:
            return jsonify({"msg": "Missing form data in request"}), 400
    else:
        error_msg = 'Invalid method'
        return error_msg


if __name__ == '__main__':
    app.run()
