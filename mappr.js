/*jslint node: true */
"use strict";

/// node.js script
/// 2013-05-08 bruslim1@gmail.com
/// TSQL merge/select-insert ETL script generator
/// https://gist.github.com/bruslim/5544909

/// MAPPING OBJECT SCHEMA:
/*
var mappings = {
	databases: {
		source: 'SourceDbName',
		destination: 'DestinationDbName'
	},
	tables: [
		{	
			destination: 'DestinationTableName',
			source: 'ViewName',
			truncate: true,
			killWhere: '',
			columns: {
				'DestinationColumnName1' : myMapper.DirectCopy({
					sourceColumn: 'SourceColumnName'			
				}),
				'DestinationColumnName2' : myMapper.ForeignKey({	
					sourceColumn: 'SourceIdColumnName',					
					parentTable:  'DestinationParentTableName',
					parentColumn: 'DestinationParentIdColumnName',
					parentType: 'char',
					importColumn: 'orig_id',
					importType: 'INT'
				}),
				'DestinationColumnName3' : myMapper.SqlFunction({
					name: 'tempFunction',
					params: [
						'SourceColumnName',
						'SourceColumnName',
						'SourceColumnName',
						'SourceColumnName'
					]
				})
			}
		}
	]
};
*/

/// how to use
/*

1. create a file named etl.js
2. import mappr.js: Mappr = require('./mappr.js')
3. create your mappings object, following the schema above
4. call Mappr.GenerateSql(mappings,mappr);
5. via command line: node etl.js > out.sql

*/

var Crypto = require('crypto');
var extend = require('node.extend');

function Config() {
	var config = {
		mappr: new exports.Mappr(),
		databases: {},
		options: {},
		modes: {
			sqlTest: false, // test mode
			passes: 1		// number of passes
		},
		tables: []
	};
	config.useMapping = function (mapping, mapArguments) {
		this.tables.push(mapping.Map(this, mapArguments));
	};
	return config;
}

exports.BuildConfig = function (source, destination, options) {
	if (arguments.length === 0) {
		throw 'Mappr.BuildConfig requires at least 1 parameter';
	}
	var config = new Config();
	if (arguments.length > 1) {
		config.databases.source = source;
		config.databases.destination = destination;
		config.options = options;
	} else {
		config = extend(config, arguments[0]);
	}
	return config;
};

exports.Mappr = function () {
	function buildLookupFunctionName(options) {
		var temp = [
			'dbo.get',
			options.parentTable,
			options.parentColumn,
			'by',
			options.importColumn
		];
		return temp.join('_');
	}
	function normalizeOptions(options) {
		var opt = extend({
			sourceColumn: '',
			value: '',
			sql: ''
		}, options);
		if (!options) { return opt; }
		return opt;
	}
	var fkLookups = { };
	var fkLookupsCount = 0;
	var fkFunctionNames = [];
	var self = {	
		CastAs: function(options, type) {
			if (!options.type) {
				options.type = type;
			}
			var opts = normalizeOptions(options);
			opts.isColumn = true;					
			return {
				options: opts,
				transform: function (tableConfig) {
					var col = self.DirectCopy(options).transform.call(null, tableConfig);
					
					return 'CAST(' + col + ' AS ' + options.type + ')';
				}
			};
		},
		AggregateColumn: function(options) {
			var opts = normalizeOptions(options);
			opts.aggregate = options.aggregate;
			opts.isColumn = false;
			return {
				options: opts,			
				transform: self.DirectCopy(options).transform	
			};
		},
		CaseMap: function(options) {	
			var opts = normalizeOptions(options);
			opts.isColumn = true;
			if (!opts.sourceColumn) { opts.sourceColumn = options; }
			return {
				options: opts,
				transform: function(tableConfig) {
					var qualifiedTarget = self.DirectCopy(options).transform.call(null, tableConfig);
					var cases = [];
					for (var value in options.map) {
						cases.push(
							"CASE WHEN " + qualifiedTarget + " = " + self.RawValue(value).transform.call(null, tableConfig) +
                            " THEN " + self.RawValue(options.map[value]).transform.call(null, tableConfig));				
					}
					var ends = [];
					for(var i =0; i < cases.length; i++) {
						ends.push("END");
					}
					return cases.join(' ELSE ')	+
                        " ELSE " + self.RawValue(options['default']).transform.call(null, tableConfig) +
                        ' ' + ends.join(' ');
				}
			};
		},
		MergeOn: function(options) {
			var opts = normalizeOptions(options);			
			var column = null;
			var raw = null;
			if (options.value !== undefined) {
				if (options.value !== null) {
					raw = '\'' +  options.value + '\'';
				}				
			} else {				
				column =  '[' + (options.sourceColumn || options) + ']';
				if (!opts.sourceColumn) { opts.sourceColumn = options; }
				opts.isColumn = true;
			}			
			return {
				options: opts,
				transform: function(tableConfig) {
					return {
						mergeOn: true,
						column: column,
						raw: raw
					};
				}
			};
		},
		InvertBit: function(options) {
			var opts = normalizeOptions(options);	
			if (!opts.sourceColumn) { opts.sourceColumn = options; }
			opts.isColumn = true;
			return {
				options: opts,
				transform: function(tableConfig) {
					var qualifiedTarget = self.DirectCopy(options).transform.call(null, tableConfig);
					return 'CASE WHEN ' + qualifiedTarget + ' = 1 THEN 0 ELSE 1 END'; 
				}
			};
		},
		TruncateString: function(options) {
			var opts = normalizeOptions(options);	
			if (!opts.sourceColumn) { opts.sourceColumn = options; }
			opts.isColumn = true;
			return {
				options: opts,
				transform: function(tableConfig) {
					var qualifiedTarget = self.DirectCopy(options).transform.call(null, tableConfig);
					var truncated = 'RTRIM(LEFT(' + qualifiedTarget + ', ' + options.stringLength + '))';				 
					return 'CASE WHEN LEN(' + truncated + ') = 0 THEN NULL ELSE ' + truncated + ' END';
				}
			};
		},
		ConcatColumns: function(options) {		
			var opts = normalizeOptions(options);	
			if (!opts.sourceColumn) { opts.sourceColumn = options; }
			opts.isColumns = true;
			return {
				options: opts,
				transform: function(tableConfig) {				
					var select = '';
					var joiner = ' + \'' + options.spacer + '\' + ';
					for(var i in options.columns) 
					{	
						var qualifiedTarget = self.DirectCopy(options.columns[i]).transform.call(null, tableConfig);
						select += 'COALESCE(' + qualifiedTarget + ', \'\')' + joiner;
					}
					var last = select.lastIndexOf(joiner);
					select = select.substring(0,last);
					return 'RTRIM(LTRIM(' +  select.trim() + '))';
				}
			};
		},
		StaticHash: function(options) {						
			var hash = Crypto.createHash(options.algorithm || 'sha512');
			hash.update(options.value || options);			
			var digest = hash.digest(options.encoding || 'base64');
			return {
				options: normalizeOptions(options),
				transform: function() {
					return self.RawValue(digest).transform.apply();
				}
			};
		},
		DirectCopy: function(options) {
			var opts = normalizeOptions(options);	
			if (!opts.sourceColumn) { opts.sourceColumn = options; }
			opts.isColumn = true;			
			return {
				options: opts,
				transform: function(tableConfig) {
					var qualifiedTarget = '[' + (options.sourceColumn || options) + ']';
					if(tableConfig && tableConfig.useMerge) {
						qualifiedTarget = 'S.' + qualifiedTarget;
					}			
					return qualifiedTarget;
				}
			};
		},
		CopyOrClear: function(options) {			
			var opts = normalizeOptions(options);	
			if (!opts.sourceColumn) { opts.sourceColumn = options; }
			opts.isColumn = true;		
			return {
				options: opts,
				transform: self.CopyOrDefault(opts).transform
			};
		},
		CopyOrDefault: function(options) {
			var opts = normalizeOptions(options);	
			if (!opts.sourceColumn) { opts.sourceColumn = options; }
			opts.isColumn = true;
			return {
				options: opts,
				transform: function(tableConfig) {
					var col = self.DirectCopy(options).transform.call(null, tableConfig);
					return 'CASE WHEN LEN(' + col + ') = 0 OR ' + col + ' IS NULL THEN ' + self.RawValue(options.value).transform.call(null,tableConfig) +' ELSE ' + col + ' END';
				}
			};
		},
		ForeignKey: function(options) {
			// creates a unique function which
			// queries the destination table by the imported column name
			// #getParentTableParentColumnByImportColumn
			// select [parentColumn] from [parentTable] where [importColumn] = @value		
			var opts = normalizeOptions(options);		
			opts.isColumn = true;	
			var fName = buildLookupFunctionName(options);
			if (!fkLookups[fName]) {
				fkLookupsCount++;
				fkLookups[fName] = options;			
			}			
			return {
				options: opts,
				transform: function(tableConfig) {
					return self.SqlFunction({
						name: fName,
						columns: [
							options.sourceColumn
						],
						raws: (options.filterColumn ? [ options.filterValue ] : null)
					}).transform.call(null, tableConfig);
				}
			};			
		},
		SqlFunction: function(options) {
			// creates the following select
			// functionName([colName],[colName],[colName]...,[rawValue],[rawValue])
			var opts = normalizeOptions(options);				
			opts.isColumns = true;	
			return {
				options: normalizeOptions(options),
				transform: function(tableConfig) {
					var formatted = [];
					for (var name in options.columns) {
						formatted.push(self.DirectCopy(options.columns[name]).transform.call(null, tableConfig));
					}
					for (var raw in options.raws) {
						formatted.push(self.RawValue(options.raws[raw]).transform.call(null, tableConfig));
					}
					if (options.name) {
						return options.name + '(' + formatted.join(',') + ')';
					}
					return options + '()';
				}
			};
		},
		Sql: function(options) {			
			var opts = {};
			if (!options.sql) {
				opts.sql = options;
			} else {
				opts = options;
			}			
			if (options.columns) {
				opts.isColumns = true;
				opts.columns = options.columns;
			}
			if (options.sourceColumn) {
				opts.isColumn = true;
				opts.sourceColumn = options.sourceColumn;
			}
			return {
				options: opts,
				transform: function(){
					return opts.sql;
				}
			};
		},
		RawValue: function(options) {
			return  {
				options: normalizeOptions(options),
				transform: function() {
					if (options !== undefined && options !== null) {
						return '\'' + (options.value || options) + '\'';
					}
					return 'NULL';
				}
			};
		},
		getFunctionCreateSql: function() {
			// returns the t-sql to create the functions
			var tempFuncs = [];			
			for(var fName in fkLookups) {
				var opt = fkLookups[fName];				
				fkFunctionNames.push(fName);
				var sql = 
					'CREATE FUNCTION ' + fName + '(@value AS ' + opt.importType + (opt.filterColumn ? ', @filter AS ' + opt.filterType :'') + ')\n' + 
                    'RETURNS ' + opt.parentType + '\n' +
                    'BEGIN\n' +
                    '    DECLARE @ret ' + opt.parentType + ';\n' +
                    '     SELECT @ret = ' + opt.parentColumn + '\n'	+
                    '       FROM [' + opt.parentTable + ']\n' +
                    '      WHERE [' + opt.importColumn + '] = @value' +
                    (opt.filterColumn ? '\n        AND [' + opt.filterColumn + '] = @filter;\n' : ';\n') +
                    '     RETURN @ret;\n' +
                    'END;\n' +
                    'GO\n';
				tempFuncs.push(sql);
			}
			return tempFuncs.join('\n');
		},
		getFunctionDropSql: function() {
			var ret = '';
			for(var i in fkFunctionNames) {
				ret += 'DROP FUNCTION ' + fkFunctionNames[i] + ';\n';
			}
			return ret;
		},
		hasLookupFunctions: function() {
			return fkLookupsCount > 0;
		}
	};
	return self;
};