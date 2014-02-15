/*jslint node: true */

'use strict';

/// imports users
exports.Users = function(config) {
	config.useMapping(require('./mappings/users.js'));	
};


/// imports roles
exports.Roles = function(config) {
	config.useMapping(require('./mappings/user_roles.js'));
};

