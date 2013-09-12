import json
import requests

from flask import Blueprint, current_app, request, session, redirect, url_for, render_template, abort

from flask.ext.login import (current_user, login_required, login_user, logout_user,
							confirm_login, fresh_login_required)

bp = Blueprint("persona", __name__, static_folder='static')

class PersonaLogin(object):
	"""
	Flask extension to manage SignIn/SignOut with Mozilla Persona.

	It requires Flask-Login to be initialized before.
	"""
	def __init__(self, app=None, url_prefix="/persona", signout_endpoint="index", signin_endpoint="signin"):
		self._url_prefix = url_prefix
		self.signout_endpoint = signout_endpoint
		self.signin_endpoint = signin_endpoint
		if app is not None:
			self.init_app(app)

	def init_app(self, app):
		app.register_blueprint(bp, url_prefix=self._url_prefix)
		app.login_manager.login_view = "persona.signin"
		app.persona = self

_VERIFIER_URL = "https://verifier.login.persona.org/verify"

# Sign In/Out ----------------------------------------------------------------------------------------------------------

@bp.route("/signout")
@login_required
def signout():
	logout_user()
	if "email" in session:
		del session["email"]
	return redirect(url_for(current_app.persona.signout_endpoint))

@bp.route("/signin", methods=["GET", "POST"])
def signin():
	if request.method == "POST":
		# The request has to have an assertion for us to verify
		if "assertion" not in request.form:
			abort(400)

		# Send the assertion to Mozilla's verifier service.
		data = {
			"assertion": request.form["assertion"],
			"audience": request.host
		}

		#current_app.logger.debug("Validation data: {0}".format(repr(data)))

		resp = requests.post(_VERIFIER_URL, data=data, verify=True)

		# Did the verifier respond?
		if resp.ok:
			# Parse the response
			verification_data = json.loads(resp.content)

			#current_app.logger.debug("Verification data = {0}".format(repr(verification_data)))

			# Check if the assertion was valid
			if verification_data["status"] == "okay":
				email = verification_data["email"]
				user = current_app.wok.get_user_by_email(email)

				if user is not None:
					# Log the user in by setting a secure session cookie
					session.update({"email" : email})
					force = False
				else:
					current_app.logger.debug("Registering user {0} ...".format(email))
					user = current_app.wok.register_user(nick=email, email=email)
					force = True

				if user is None:
					current_app.logger.error("Registration of user {} failed".format(email))
					abort(500)

				if login_user(user, force=force):
					current_app.logger.debug("User {0} signed in".format(email))
					return resp.content

	return redirect(url_for(current_app.persona.signin_endpoint, next=request.args.get("next")))