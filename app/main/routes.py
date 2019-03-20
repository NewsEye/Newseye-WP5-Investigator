from flask import jsonify
from flask_login import login_required
from app.main import bp, core


@bp.route('/history')
@login_required
def get_history():
    history = core.get_history()
    return jsonify(history)
