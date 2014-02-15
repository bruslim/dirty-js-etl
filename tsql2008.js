/// this file generates tsql compatiable with sqlserver2008+

/*jslint node: true */

'use strict';

var getFullyQualifiedName = function(dbName,tblName) {
	if (tblName.indexOf('#') > -1) { return tblName; }
	return '[' + dbName + '].[dbo].[' + tblName + ']';
};

var getFullyQualifiedDestination = function(config, tbl) {
	return getFullyQualifiedName(config.databases.destination, tbl.destination);
};

var getFullyQualifiedSource = function(config, tbl) {
	return getFullyQualifiedName(config.databases.source, tbl.source);		
};

var sqlMsg = function(message) {	
	return "PRINT('" + message.replace(/'/g,"''") + "');";
};

var sqlComment = function(sql, isNotCommented) {
	if (isNotCommented) { return sql; }
	return '-- ' + sql;
};

var sqlSetup = (function(){
	var core = function(config) {
		var mappr = config.mappr;
		
		console.log(sqlMsg('======== BEGIN SETUP ========'));
		console.log(sqlMsg(''));
		
		console.log(sqlMsg('SETUP: Switching to [' + config.databases.destination + ']'));
		console.log('USE [' + config.databases.destination + '];');	
		
		if(mappr.hasLookupFunctions()) {			
			console.log('-- BEGIN CREATE FUNCTIONS --\n');
			
			console.log(sqlMsg('SETUP: Creating look-up functions'));
			console.log(sqlMsg(''));
			console.log('GO\n');
			console.log(mappr.getFunctionCreateSql() + '\n');
			
			console.log('-- END CREATE FUNCTIONS --\n');			
		}
		
		console.log(sqlMsg('SETUP: Executing Setup Commands'));
		console.log(sqlMsg(''));
		var setupCmds = [];
		for(var t in config.tables) {
			var tbl = config.tables[t];
			if (!tbl.setup || setupCmds.indexOf(tbl.setup) > -1) { continue; }
			setupCmds.push(tbl.setup);
			console.log(tbl.setup);		
		}
		console.log('');
		console.log(sqlMsg('========= END SETUP ========='));
		console.log(sqlMsg(''));	
	};
	
	var testing = function(config) {
		if (!config.modes.sqlTest) { return; }
		console.log(sqlMsg('==================================================='));
		console.log(sqlMsg('=!!!!! TEST MODE: TRANSACTION IS ROLLED BACK !!!!!='));
		console.log(sqlMsg('==================================================='));
		console.log(sqlMsg(''));
		console.log('BEGIN TRANSACTION;');		
	};
	
	var cleanup = function(config) {
		console.log('\n-- BEGIN CLEAN UP SQL --\n');
		console.log(sqlMsg('======== BEGIN CLEANUP ========'));
		console.log(sqlMsg(''));
		var cmds = [],
            cmd  = '';
		// delete things in reverse order
		for(var i = config.tables.length-1; i >= 0; --i) {			
			var tbl             = config.tables[i],
				fullDestination = getFullyQualifiedDestination(config, tbl),
				fullSource      = getFullyQualifiedSource(config,tbl);
			
			if (tbl.truncate && !tbl.killWhere) {                
				cmd = 'TRUNCATE TABLE ' + fullDestination + ';';
				if (cmds.indexOf(cmd) < 0) {
					console.log(sqlComment(sqlMsg('INFO: Truncating table ' + fullDestination), !tbl.useMerge));
					console.log(sqlComment(cmd, !tbl.useMerge));
					console.log(sqlComment(sqlMsg(''), !tbl.useMerge));
					cmds.push(cmd);
				}
			}
			
			if (!tbl.truncate && tbl.killWhere) {
				cmd = 'DELETE FROM ' + fullDestination + ' WHERE ' + tbl.killWhere +';';
				if (cmds.indexOf(cmd) < 0) {
					console.log(sqlComment(sqlMsg('INFO: Deleting all records in ' + fullDestination + ' where ' + tbl.killWhere), !tbl.useMerge));
					console.log(sqlComment(cmd, !tbl.useMerge));			
					console.log(sqlComment(sqlMsg(''), !tbl.useMerge));
					cmds.push(cmd);
				}
			}				
		}
		console.log(sqlMsg('========= END CLEANUP ========='));
		console.log(sqlMsg(''));
		console.log('\n-- END CLEAN UP SQL --\n');
	};
	
	return function(config) {			
		core(config);
		testing(config);
		cleanup(config);
	};
})();


var sqlTeardown = (function(){
	var core = function(config) {
		console.log(sqlMsg('======== BEGIN TEARDOWN ========'));
		console.log(sqlMsg(''));
		console.log('GO\n');
		
		if (config.mappr.hasLookupFunctions()) {
			console.log('-- BEGIN DROP FUNCTIONS --\n');			
			console.log(sqlMsg('TEARDOWN: Dropping Look-up Functions'));
			console.log(sqlMsg(''));
			console.log('GO\n');
			console.log(config.mappr.getFunctionDropSql());	
			console.log('-- END DROP FUNCTIONS --\n');		
		}
		
		console.log(sqlMsg('TEARDOWN: Executing Teardown Commands'));
		console.log(sqlMsg(''));
		var teardownCmds = [];
		for(var t in config.tables) {
			var tbl = config.tables[t];
			if (!tbl.teardown || teardownCmds.indexOf(tbl.teardown) > -1) { continue; }
			teardownCmds.push(tbl.teardown);
			console.log(tbl.teardown);
		}
		
		console.log(sqlMsg('========= END TEARDOWN ========='));		
		
	};
	
	var testing = function(config) {
		if(!config.modes.sqlTest) { return; }
		console.log(sqlMsg('==================================================='));
		console.log(sqlMsg('=!!!!!! TEST MODE: ROLLING BACK TRANSACTION !!!!!!='));		
		console.log(sqlMsg('==================================================='));
		console.log('rollback;');
		console.log(sqlMsg(''));	
	};
	
	return function(config) {
		testing(config);
		core(config);		
	};
})();

var sqlBody = (function() {
	var migrateTable = function(config, tbl) {				
			
		var processed = processConfig(config,tbl);
			
		if (tbl.useMerge) {
			migrateWithMerge(tbl, processed);	
		} else {		
			migrateWithInsert(tbl, processed);	
		}

		console.log(sqlMsg('') + '\n');	
		
	};
	
	var processConfig = function(config, tbl) {
		var destColNames = [],				
			srcCols      = [],
			mergeOn      = [],
			groupbys     = [],
			aggregates   = [];
			
		// for each column name in tbl.columns
		for(var colName in tbl.columns) {		
			// add the colName to the list
			var destCol = '[' + colName + ']';
			destColNames.push(destCol);
			
			// source mapping
			var srcMapping = tbl.columns[colName];
			
			// transform the source
			var srcCol = srcMapping.transform.call(null, tbl);
			
			// handle merge
			if (srcCol.mergeOn) {					
				if (srcCol.column) {
					srcCols.push('S.' + srcCol.column);						
					mergeOn.push('D.' + destCol + ' = S.' + srcCol.column);
				} else {						
					if (!!srcCol.raw) {			
						srcCols.push(srcCol.raw);
						mergeOn.push('D.' + destCol + ' = ' + srcCol.raw);
					} else {
						srcCols.push('NULL');
						mergeOn.push('D.' + destCol + ' IS NULL');
					}
				}				
			} else {			
				srcCols.push(srcCol);
			}	
			
            var qualifiedInner = '';
			// handle grouping for single column options
			if (tbl.useGrouping && srcMapping.options.isColumn) {					
				qualifedInner = '[' + srcMapping.options.sourceColumn + ']';
				if (groupbys.indexOf(qualifedInner) < 0) {
					groupbys.push(qualifedInner);
				}
			}
			
			// handle grouping for multi-column options
			if (tbl.useGrouping && srcMapping.options.isColumns) {			
				var innerCols = [];
				for(var ic in srcMapping.options.columns) {
					qualifedInner = '[' + srcMapping.options.columns[ic] + ']';
					if (groupbys.indexOf(qualifedInner) < 0) {
						groupbys.push(qualifedInner);
					}						
				}						
			}
			
			// handle grouping w/ aggregate function
			if (tbl.useGrouping && srcMapping.options.aggregate) {	
				var qualifedInner = '[' + srcMapping.options.sourceColumn + ']';
				aggregates.push(srcMapping.options.aggregate + '(' + qualifedInner + ') AS ' + qualifedInner);
			}
		}	
		
		return {
			fullDestination: getFullyQualifiedDestination.call(null, config, tbl),
			fullSource     : getFullyQualifiedSource.call(null, config, tbl),
			destColNames   : destColNames,				
			srcCols        : srcCols,
			mergeOn        : mergeOn,
			groupBys       : groupbys,
			aggregates     : aggregates
		};
	};
	
	var migrateWithMerge = function(tbl, p) {	
		console.log(sqlMsg('==== Merging ' + p.fullDestination + ' with ' + p.fullSource + ' ====='));
		if (tbl.message) {
			console.log(sqlMsg('INFO: ' + tbl.message));
		}
		// begin sql MERGE
		console.log('MERGE ' + p.fullDestination + ' AS D');
		// begin sql MERGE USING clause
		var using = 'USING ';
		// force SELECT if needed
		if (tbl.sourceFilter || tbl.useGrouping || tbl.forceDistinct) {				
			using += '(\n    SELECT ';
			// force distinct
			if (tbl.forceDistinct) {
				using += 'DISTINCT\n           ';
			}
			// select group by cols or *
			var groupByCols =  p.groupBys.join(',\n           '),
				aggregateCols = p.aggregates.join(',\n           ');
			if (tbl.useGrouping) {					
				using += groupByCols + (p.aggregates && p.aggregates.length > 0 ? ',\n           ' + aggregateCols : '');
			} else {
				using += '*';
			}			
			// select FROM
			using += '\n      FROM ' + p.fullSource;
			// WHERE CLAUSE
			if (tbl.sourceFilter) {
				using += '\n     WHERE ' + tbl.sourceFilter;
			}
			// group by clause
			if (tbl.useGrouping) {
				using += '\n  GROUP BY ' + groupByCols;				
			}
			using += '\n   )';				
		} else {
			using += p.fullSource;				
		}
		using += ' AS S';
		console.log(using);
		console.log('   ON ' + p.mergeOn.join('\n      AND '));
		
		var sets = [],
			inserts = [];
		for(var i in p.destColNames) {
			// do not update pks! 
			if (p.destColNames[i] != '[' + tbl.mergePk + ']') {
				sets.push('    ' + p.destColNames[i] + ' = ' + p.srcCols[i]);
			}				
			inserts.push('Inserted.' + p.destColNames[i]);				
		}
		
		if (!tbl.insertOnly) {
			console.log('WHEN MATCHED THEN');
			console.log('    UPDATE SET');
			console.log(sets.join(',\n'));
		}
		
		if (!tbl.updateOnly) {
			console.log('WHEN NOT MATCHED THEN');			
			console.log('    INSERT (\n        ' + p.destColNames.join(',\n        ') + '\n    ) VALUES (');
			console.log('        ' + p.srcCols.join(',\n        ') + '\n    )');
		}
		
		console.log('OUTPUT $action,\n       ' +  inserts.join(',\n       ') + ';');	
	};

	var migrateWithInsert = function(tbl, p)  {
		console.log(sqlMsg('==== Inserting into ' + p.fullDestination + ' with ' + p.fullSource + ' ====='));				
		if (tbl.message) {
			console.log(sqlMsg('MSG: ' + tbl.message));
		}
		var sql = 'INSERT INTO ' + p.fullDestination + ' (\n';
		sql += p.destColNames.join(',\n');
		sql += '\n) SELECT ';
		if (tbl.forceDistinct) {
			sql += 'DISTINCT\n           ';
		} else {
			sql += '\n' + p.srcCols.join(',\n');
			if (tbl.useGrouping && p.aggregates && p.aggregates.length > 0) {
				sql += p.aggregates.join(',\n');
			}
		}
		sql += '\nFROM ' + p.fullSource;
		if (tbl.sourceFilter) {
			sql += '\nWHERE ' + tbl.sourceFilter;
		}		
		if (tbl.useGrouping) {
			sql += '\nGROUP BY ' + p.groupBys.join(', ');	
		}
		console.log(sql + ';\n');	
	};

	return function(config) {
		console.log('\n-- BEGIN INSERT-SELECT SQL --\n');
		for(var pass = 0; pass < config.modes.passes; pass++) {
			// for each table name in config.tables
			console.log(sqlMsg('======== BEGIN PASS ' + (pass + 1) + ' of ' + config.modes.passes + ' ========'));
			console.log(sqlMsg(''));
			for(var i in config.tables) {				
				migrateTable(config, config.tables[i]);
			}
			console.log(sqlMsg('======= END OF PASS ' + (pass + 1) + ' of ' + config.modes.passes + ' ========'));
			console.log(sqlMsg(''));		
		}
		console.log('-- END INSERT-SELECT SQL -- \n');
	};
})();

exports.GenerateSql = function(config) {	
	sqlSetup(config);	
	sqlBody(config);	
	sqlTeardown(config);
};