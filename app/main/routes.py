from flask import jsonify
from flask_login import login_required
from app.assistant import core
from app.main import bp


@bp.route('/history')
@login_required
def get_history():
    history = core.get_history()
    return jsonify(history)


@bp.route('/test/multiquery')
@login_required
def test_multiquery():
    test_query = [
        {'q': ['lighthouse']},
        {'q': ['ghost']}
    ]
    return jsonify(core.run_query_task(test_query))
