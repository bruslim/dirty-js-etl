/*jslint node: true */

'use strict';

exports.Map = function(config) {
	var mappr = config.mappr,
		useEmptyPassword = config.options.clearPasswords;	
	return {
		destination: 'users',
		source: 'etl_Users',
		useMerge: true,		
		killWhere: 'orig_id IS NOT NULL',
		message: 'migrating NON-DUPLICATE users',
		columns: {
			'orig_id': mappr.MergeOn('UserID'),
			'orig_admin': mappr.DirectCopy('Admin'),
			'user_id': mappr.DirectCopy('Username'),			
			'password': (function(useEmptyPassword) { 
				if (useEmptyPassword) {
					// clear password
					return mappr.RawValue(null);
				}
				return mappr.DirectCopy('Password');				
			})(useEmptyPassword),
			'user_nm': mappr.ConcatColumns({
				columns: ['FirstName','LastName'],
				spacer: ' '
			}),
			'attr_bits': mappr.InvertBit('IsActive'),
			'email_address': mappr.DirectCopy('EMailAddress'),	
			'user_guid': mappr.ForeignKey({  // this is the individual guid fk
				sourceColumn: 'AgentID',
				parentTable: 'individual',
				parentColumn: 'individual_id',
				parentType: 'UNIQUEIDENTIFIER',
				importColumn: 'orig_agent_id',
				importType: 'INT'
			}),
			'first_name': mappr.DirectCopy('FirstName'),
			'last_name': mappr.DirectCopy('LastName'),
			'date_created': mappr.DirectCopy('RegistrationDate'),	
			'pin_code': mappr.DirectCopy('PinCode')
		}
	};
};
