/**
 * Created by Administrator on 2016/6/4.
 */
"use strict"
const assert = require("assert");
const mysql = require('mysql');
const merge = require("lodash").merge;

const constants = require("./../constants");
const SQLTransformer = require("./../utils/sqlTransformer");
const resultTransformer = require("./../utils/resultTransformer");

const _DEBUG_SQL = process.env.DEBUG;

function convertTypeDefToDbFormat(typeDef) {
	if(typeof typeDef === "object") {
		switch (typeDef.type) {
			case "text":
				return typeDef.type;
			case "string": {
				return "varchar(" + new String(typeDef.size || 32) + ")";
			}
			case "int":
			case "integer": {
				switch(typeDef.size) {
					case 1:
						return "tinyint";
					case 2:
						return "smallint";
					case 3:
						return "mediumint";
					case 4:
						return "int";
					case 8:
						return "bigint";
					default:
						throw new Error("int类型只支持1、2、4、8的长度");
				}
			}
			case "num":
			case "number": {
				return "double(" + typeDef.M + "," + typeDef.D + ")";
			}
			case "boolean": {
				return "tinyint";
			}
			case "date" :
			case "time": {
				return "bigint";
			}
			case "array":
			case "object":
			case "file":
			case "image":
			case "img": {
				return "text";
			}
			case "serial": {
				return "bigint auto_increment";
			}
			case "enum": {
				let format =  "enum(";
				if(typeDef.values && typeDef.values instanceof Array) {
					typeDef.values.forEach((ele, index) => {
						if(index !== 0) {
							format += ",";
						}
						format += "\"";
						format += ele;
						format += "\"";
					});
					format += ")";
					return format;
				}
				else {
					throw new Error("无效的enum values定义")
				}
			}
			case "loc":
			case "geo":
			case "geometry": {
				return "GEOMETRY";
			}
			default: {
				throw new Error("不能识别的类型" + typeDef.type);
			}
		}
	}
	else if(typeof typeDef === "string") {
		let typeDef2 = {};
		typeDef2.type = typeDef;
		typeDef2.size = (typeDef === "string") ? 32 : 4;
		typeDef2.M = 20;
		typeDef2.D = 4;
		return convertTypeDefToDbFormat(typeDef2);
	}
}

const GeoTypes = [
	{
		name: "Point"
	},
	{
		name: "LineString",
		element: "Point"
	},
	{
		name: "MultiLineString",
		element: "LineString"
	},
	{
		name: "Polygon",
		element: "LineString"
	},
	{
		name: "MultiPoint",
		element: "Point"
	},
	{
		name: "MultiPolygon",
		element: "Polygon"
	}
];

function transformGeoData(data) {
	if(data.type.toLowerCase() === "geometrycollection") {
		let result = "GeometryCollection(";
		data.coordinates.forEach(
			(ele, idx) => {
				if(idx > 0) {
					result += ",";
				}
				result += transFormGeoData(ele);
			}
		);
		result += ")";
		return result;
	}
	else {
		let type = GeoTypes.find(
			(ele) => {
				return ele.name.toLowerCase() === data.type.toLowerCase()
			}
		);
		if(!type) {
			throw new Error("不能识别的地理格式[" + data.type + "]");
		}
		let result = type.name + "(";
		data.coordinates.forEach(
			(ele, idx) => {
				if(idx > 0){
					result += ",";
				}
				if(type.element) {
					result += transformGeoData({
						type: type.element,
						coordinates: ele
					});
				}
				else {
					result += new String(ele);
				}
			}
		);
		result += ")";
		return result;
	}
}

function convertValueToDbFormat(value, type) {
	if(typeof type === "object") {
		type = type.type;
	}

	// 处理null或者undefined
	if(value === null || value === undefined) {
		return "NULL";
	}
	// 处理一种子查询的特殊情况
	if(typeof value === "object" && value.hasOwnProperty("$ref")) {
		/**
		 *	处理一种特殊情况，在$has这样的subquery中，用
		 *		{
			 *			$ref: Object
			 *			$attr: "id"
			 *		}
		 *	这样的格式来传属性
		 */
		assert (Object.getOwnPropertyNames(value).length === 2 && value.hasOwnProperty("$attr"));
		let result = "`" + value["$ref"]["#execNode#"]["alias"] + "`";
		result +=".";
		result += value["$attr"];
		return result;
	}

	switch (type) {
		case "string":
		case "text":
		case "enum":
		case "file":
		case "image":
		case "img": {
			if(typeof value === "string") {
				return this.db.escape(value);
			}
			else if(typeof value.toString === "function") {
				return this.db.escape(value.toString());
			}
			else {
				throw new Error("错误的" + type + "类型");
			}
		}
		case "num":
		case "number":
		case "int":
		case "integer":
		case "serial": {
			if(typeof value === "number") {
				return new String(value);
			}
			else {
				throw new Error("错误的" + type + "类型");
			}

		}
		case "date":
		case "time": {
			if(typeof value === "object" && value instanceof Date) {
				return new String(value.valueOf());
			}
			else if(typeof value === "number") {
				return new String(value);
			}
			else {
				throw new Error("不能识别的" + type + "类型");
			}
		}
		case "boolean": {
			return new String(value);
		}
		case "array":
		case "object":{
			return this.db.escape(JSON.stringify(value));
		}
		case "loc":
		case "geo":
		case "geometry": {
			// 这里将GeoJson类型的数据转换为Mysql的SQL格式
			return transformGeoData(value);
		}
		default: {
			throw new Error("不能识别的类型[" + type + "]");
		}
	}
}

function queryToPromise(db, query, transformSelectResult) {
	return new Promise((resolve, reject) => {
		const sql = db.query(query, (err, result, field) => {
			if(_DEBUG_SQL) {
				console.log("ends at: " + new Date());
				if(err) {
					console.log("err: " + err);
				}
				/*else {
				 console.log("result: " + result);
				 console.log("field: " + field);
				 }*/
				console.log("[exec sql end]");
			}
			if(err) {
				reject(err);
			}
			else {
				if(transformSelectResult) {
					resolve(resultTransformer.transformSelectResult(result));
				}
				else {
					resolve(result, field);
				}
			}
		});

		if(_DEBUG_SQL) {
			console.log("[exec sql begin]");
			console.log("begins at: " + new Date());
			console.log(sql.sql);
		}
	})
}

function getPrimaryKeyColumn(schema) {
	for(let attr in schema.attributes) {
		const attrDef = schema.attributes[attr];
		if(attrDef.key) {
			return attr;
		}
	}

	return constants.mysqlDefaultIdColumn;
}

class Mysql {
	constructor(settings, schemas) {
		this.settings = settings;
		this.schemas = schemas;
		this.sqlTransformer = new SQLTransformer(schemas, convertValueToDbFormat.bind(this));
	}


	connect(url) {
		if (typeof url === 'string') {
			this.db = mysql.createConnection(url);

			return new Promise((resolve, reject) => {
				this.db.connect((err) => {
					if(err) {
						reject(err);
					}
					else {
						resolve();
					}
				})
			});
		}
		else if (typeof url === 'object' && url.hasOwnProperty('host')) {
			this.db = mysql.createPool(url);
			return Promise.resolve();
		}
		else {
			throw new Error('不合法的url格式');
		}
	}

	setSchemas(schemas) {
		this.schemas = schemas;
		this.sqlTransformer.setSchemas(schemas);
	}

	disconnect() {
		return new Promise((resolve, reject) => {
			this.db.end((err) => {
				this.db = undefined;
				if(err) {
					reject(err);
				}
				else {
					resolve();
				}
			})
		})
	}

	find(name, execTree, indexFrom, count, isCounting, con) {
		const sql = this.sqlTransformer.transformSelect(name, execTree, indexFrom, count, isCounting);

		return queryToPromise(con || this.db, sql, true)
			.then(
				(result) => {
					if(isCounting) {
						return Promise.resolve(result[0]);
					}
					return Promise.resolve(result);
				}
			)
	}

	insert(name, data, schema, con) {
		assert(data instanceof Array && data.length >= 1);
		let query = this.sqlTransformer.transformInsert(name, data);

		return queryToPromise(con || this.db, query)
			.then(
				(result) => {
					for(let attr in schema.attributes) {
						const attrDef = schema.attributes[attr];
						if(attrDef.key === true) {
							// 如果有显式定义的主键，则给主键赋上值
							// 要支持插入多列，这里的id返回的是第一列的id，因此以1为单位递增
							data.forEach(
								(ele, idx) => {
									ele[attr] = result.insertId + idx;
								}
							);
							return Promise.resolve(data.length > 1 ? data : data[0]);
						}
					}

					data.forEach(
						(ele, idx) => {
							ele[constants.mysqlDefaultIdColumn] = result.insertId + idx;
						}
					);
					return Promise.resolve(data.length > 1 ? data : data[0]);

				},
				(err) => {
					return Promise.reject(err);
				}
			);
	}

	update(name, updatePart, query, con) {
		if(typeof updatePart != "object") {
			throw new Error("更新的数据必须是object类型");
		}

		let sql = this.sqlTransformer.transformUpdate(name, updatePart, query);

		return queryToPromise(con || this.db, sql)
			.then(
				(result) => {
					return Promise.resolve(result.affectedRows);
				},
				(err) => {
					return Promise.reject(err);
				}
			);
	}

	updateOneById(name, updatePart, id, schema, con) {
		if(typeof updatePart != "object") {
			throw new Error("更新的数据必须是object类型");
		}
		const idColumn = getPrimaryKeyColumn(schema);
		let sql = this.sqlTransformer.transformUpdate(name, updatePart, {
			[idColumn]: id
		});

		return queryToPromise(con || this.db, sql)
			.then(
				(result) => {
					if(result.affectedRows === 1) {
						// 这里只勉强返回一个更新后的域的组装对象
						return Promise.resolve(merge({}, {
							[idColumn]: id
						}, updatePart.$set));
					}
					else {
						return Promise.resolve();
					}
				},
				(err) => {
					return Promise.reject(err);
				}
			);

	}

	remove(name, query, con) {
		let sql = sqlTransformer.transformDelete(name, query);


		return queryToPromise(con || this.db, sql)
			.then(
				(result) => {
					return Promise.resolve(result.affectedRows);
				},
				(err) => {
					return Promise.reject(err);
				}
			);
	}

	removeOneById(name, id, schema, con) {
		const idColumn = getPrimaryKeyColumn(schema);
		let sql = this.sqlTransformer.transformDelete(name, {
			[idColumn]: id
		});

		return queryToPromise(con || this.db, sql)
			.then(
				(result) => {
					if(result.affectedRows === 1) {
						// 这里只勉强返回一个只有id的对象
						return Promise.resolve({
							[idColumn]: id
						});
					}
					else {
						return Promise.resolve();
					}
				},
				(err) => {
					return Promise.reject(err);
				}
			);

	}

	createSchema(schema, name, con) {
		let query = "create table if not exists `";
		query += name;
		query += "`(";
		let PrimaryKeyHasDefined = false;
		for(let attr in schema.attributes) {
			const attrDef = schema.attributes[attr];
			/* if(attr === "id" && (attrDef.type !== "serial" || !attrDef.key)) {
			 throw new Error("id列如果显式定义，必须是Serial类型且必须设置为主键");
			 }
			 */
			// 忽略所有的ref列，在传入之前UDA应该已经将之转换完毕
			if(attrDef.type === constants.typeReference) {
				continue;
			}

			if(!query.endsWith("(")) {
				query +=",";
			}
			query += "`" + attr;
			query += "` ";
			query += convertTypeDefToDbFormat(attrDef.type);
			if(attrDef.key) {
				if(!PrimaryKeyHasDefined) {
					PrimaryKeyHasDefined = true;
					query += " primary key";
				}
				else {
					throw Error("当前不允许定义联合主键");
				}
			}
			else {
				if(attrDef.hasOwnProperty("unique")) {
					query += " unique";
				}
				if(attrDef.hasOwnProperty("required")) {
					query += " not null";
				}
				if(attrDef.hasOwnProperty("default")) {
					query += " default " + convertValueToDbFormat(attrDef.default, attrDef.type);
				}
			}
		}
		/* if(!PrimaryKeyHasDefined) {
		 // 帮助定义主键
		 query += "," + constants.mysqlDefaultIdColumn + " bigint primary key auto_increment";
		 }*/

		// 视要求建立索引
		if(schema.indexes) {
			for(let index in schema.indexes) {
				const indexDef = schema.indexes[index];
				query += ",";
				if(indexDef.options && indexDef.options.unique) {
					assert(!indexDef.options.spatial && !indexDef.options.fulltext);
					query += "unique";
				}
				if(indexDef.options && indexDef.options.spatial) {
					assert(!indexDef.options.unique && !indexDef.options.fulltext);
					query += "spatial";
				}
				if(indexDef.options && indexDef.options.fulltext) {
					assert(!indexDef.options.unique && !indexDef.options.spatial);
					query += "fulltext";
				}
				query += " index `";
				query += index + "`";

				if(indexDef.columns && typeof indexDef.columns  == "object") {
					query += "(";
					for(let column in indexDef.columns) {
						if(!query.endsWith("(")) {
							query += ","
						}
						query += "`" + column + "`";
						if(indexDef.columns[column] == 1) {
							query += " ASC";
						}
						else if(indexDef.columns[column] == -1){
							query += " DESC";
						}
					}
					query += ")";
				}
				else {
					throw new Error("错误的索引列定义格式");
				}

				if(indexDef.options && indexDef.options.fulltext && indexDef.options.ngram) {
					query += " with parser ngram";
				}
			}
		}

		query += ");";



		return queryToPromise(con || this.db, query);
	}

	dropSchema(schema, name, con) {
		let query = "drop table if exists `";
		query += name + "`";

		return queryToPromise(con || this.db, query);
	}

	getDefaultKeyType() {
		return new Promise(
			(resolve, reject) => {
				resolve(
					"serial"
				);
			});
	}

	getDefaultKeyName() {
		return new Promise(
			(resolve, reject) => {
				resolve("id");
			}
		);
	}

	execSql(sql, transformResultToObject, con) {
		return queryToPromise(con || this.db, sql, transformResultToObject);
	}


	startTransaction(option) {
		const Pool = require('mysql/lib/Pool');
		if (this.db instanceof Pool) {
			return new Promise(
				(resolve, reject) => {
					this.db.getConnection((err, connection) => {
						if (err) {
							reject(err);
						}
						function startTxn() {
						    let statement = 'START TRANSACTION';
                            if (option && option.extras) {
                                statement = statement.concat(` ${option.extras}`);
                            }
                            connection.query(statement, (err, result, fields) => {
                                if(err) {
                                    reject(err);
                                }
                                else {
                                    resolve(connection);
                                }
                            })
                        }
                        if (option && option.isolationLevel) {
                            connection.query(`SET TRANSACTION ISOLATION LEVEL ${option.isolationLevel}`, (err) => {
                                if(err) {
                                    reject(err);
                                }
                                startTxn();
                            })
                        }
                        else {
                            startTxn();
                        }
					});
				}
			);
		}
		else {
			const Connection = require('mysql/lib/Connection');
			assert(this.db instanceof Connection);
			throw new Error('要支持事务，则mysql的连接必须是连接池式配置');
		}
	}

	commmitTransaction(con, option) {
		return new Promise(
			(resolve, reject) => {
			    let statement = 'COMMIT';
                if (option && option.extras) {
                    statement = statement.concat(` ${option.extras}`);
                }
				con.query(statement, (err, result, fields) => {
					if(err) {
						reject(err);
					}
					else {
					    if (option && option.isolationLevel) {
					        con.query(`SET TRANSACTION ISOLATION LEVEL ${option.isolationLevel}`, (err) => {
					            con.release();
                                resolve();
                            });
                        }
                        else {
                            con.release();
                            resolve()
                        };
					}
				});
			}
		);
	}

	rollbackTransaction(con, option) {
		return new Promise(
			(resolve, reject) => {
                let statement = 'ROLLBACK';
                if (option && option.extras) {
                    statement = statement.concat(` ${option.extras}`);
                }
				con.query(statement, (err, result, fields) => {
					if(err) {
						reject(err);
					}
					else {
                        if (option && option.isolationLevel) {
                            con.query(`SET TRANSACTION ISOLATION LEVEL ${option.isolationLevel}`, (err) => {
                                con.release();
                                resolve();
                            });
                        }
                        else {
                            con.release();
                            resolve()
                        };
					}
				});
			}
		);
	}
}


module.exports = Mysql;