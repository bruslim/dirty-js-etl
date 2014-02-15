/*jslint node: true */

'use strict';

// requires
var Mappr = require('./mappr.js');
var Tsql2008 = require('./tsql2008.js');
var testing = true;

var config = Mappr.BuildConfig({
	databases : {
		// source db name
		source: 'source_db',
		// desitnation db name
		destination: 'destination_db'
	},
	options : {
		// option to clear passwords (for testing purposes)
		clearPasswords: true
	}
});

if (testing) {
	config.modes = {
		// flag for test mode
		sqlTest: true,
		// number of passes for the import script to run
		// typically 1 for production (insert and update), 2 for testing (insert and update, then update again)
		passes: 2
	};
} else {
	config.modes = {
		// flag for test mode
		sqlTest: false,
		passes: 1
	};
}

// load all the tables in the order which they have been exported
var Tables = require('./tables.js');
for (var t in Tables) {
	Tables[t].call(null, config);
}

//Tables.RatesSelected(config);

// gen the sql script to the console
Tsql2008.GenerateSql(config);
