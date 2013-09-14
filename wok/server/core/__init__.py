from flask import Blueprint

bp = Blueprint("core", __name__, static_folder='static', template_folder="templates")
