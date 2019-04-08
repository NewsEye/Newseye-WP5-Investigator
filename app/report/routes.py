from flask import jsonify
from flask_login import login_required
from app.report import bp
from app.main.misc_utils import get_history


@bp.route('/history/')
@login_required
def history():
    return jsonify(get_history())
