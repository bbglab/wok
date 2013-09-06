function persona_init(conf) {
	navigator.id.watch({
		loggedInUser: conf.user_id,
		onlogin: function(assertion) {
			// A user has logged in! Here you need to:
			// 1. Send the assertion to your backend for verification and to create a session.
			// 2. Update your UI.
			$.ajax({
				type: 'POST',
				url: conf.login_url,
				data: {assertion: assertion},
				success: function(res, status, xhr) { window.location.replace(conf.next_url); },
				error: function(xhr, status, err) { alert("Sign in failure: " + err); }
			});
		},
		onlogout: function() {
			// A user has logged out! Here you need to:
			// Tear down the user's session by redirecting the user or making a call to your backend.
			// Also, make sure loggedInUser will get set to null on the next page load.
			// (That's a literal JavaScript null. Not false, 0, or undefined. null.)
			$.ajax({
				type: 'GET',
				url: conf.logout_url,
				success: function(res, status, xhr) { window.location.replace(conf.next_url); },
				error: function(xhr, status, err) { alert("Sign out failure: " + err); }
			});
		}
	});

	$(conf.login_button).button().click(function (event) {
		event.preventDefault();
		navigator.id.request();
	});

	$(conf.logout_button).button().click(function (event) {
		event.preventDefault();
		navigator.id.logout();
	});
}