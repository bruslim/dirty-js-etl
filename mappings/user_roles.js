/*jslint node: true */

'use strict';

exports.Map = function(config) {
	var mappr = config.mappr;
	return {
		destination: 'user_roles',
		source: 'nq_etl_Users',
		killWhere: 'orig_user_id IS NOT NULL',
		useMerge: true,		
		message: 'defaulted role for imported to Users',
		columns: {
			'orig_user_id': mappr.MergeOn('UserID'),
			'user_id': mappr.DirectCopy('Username'),	
			/*
			'user_id': mappr.ForeignKey({
				sourceColumn: 'UserID',
				parentTable:  'users',
				parentColumn: 'user_id',
				parentType: 'NVARCHAR(64)',
				importColumn: 'orig_id',
				importType: 'INT'
			}),
			*/			
			'role_nm': mappr.RawValue('Users')
		}
	};
};