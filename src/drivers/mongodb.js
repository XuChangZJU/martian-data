/**
 * Created by Administrator on 2016/6/4.
 */
"use strict";

const MongoClient = require('mongodb').MongoClient;
const ObjectId = require("mongodb").ObjectID;
const constants = require("./../constants");

const assert = require("assert");
const merge = require("lodash").merge;


/**
 * 从mongo返回的数据，一定要把_id先强制转换成string类型，保持全局的统一
 * @param data
 */
function formalizeResult(data) {
	/*let data2 = Object.assign({}, data);
	 if(data2._id instanceof ObjectId) {
	 data2._id = data2._id.toString();
	 return data2;
	 }*/
	return data;
}


function formalizeObjectIdinPredicate(obj) {
	if(obj instanceof ObjectId) {
		return;
	}
	for(let predicate in obj) {
		if(!predicate.startsWith("$")) {
			throw new Error("可能遇见了不能处理的语义");
		}
		else {
			if(typeof obj[predicate] === "string" && query._id.length === 24) {
				obj[predicate] = new ObjectId(obj[predicate]);
			}
			else if(typeof obj[predicate] === "object" && obj[predicate] instanceof Array) {
				obj[predicate] = obj[predicate].map(
					(ele) => {
						if(typeof ele === "string" && ele.length === 24) {
							return new ObjectId(ele);
						}
						else {
							return ele;
						}
					}
				);
			}
		}
	}
}

function formalizeQuery(query) {
	if(query.$and) {
		query.$and.map(
			(ele) => {
				formalizeQuery(ele);
			}
		);
	}
	if(query.$or) {
        query.$or.map(
            (ele) => {
                formalizeQuery(ele);
            }
        );
	}
	if(query.$nor) {
        query.$nor.map(
            (ele) => {
                formalizeQuery(ele);
            }
        );
	}

	if(query._id) {
		if(typeof query._id === "string" && query._id.length === 24) {
			query._id = new ObjectId(query._id);
		}
		else if(typeof query._id === "object") {
			formalizeObjectIdinPredicate(query._id);
		}
	}
}

function getCollection(name) {
	return new Promise((resolve, reject) => {
		if(this.collections[name]) {
			resolve(this.collections[name]);
		}
		else {
			this.db.collection(name, (err, collection) => {
				if(err) {
					reject(err);
				}
				else if(collection) {
					this.collections[name] = collection;
					resolve(collection);
				}
				else {
					reject("没有找到相应的表对象" + name);
				}
			});
		}
	})
}

/**
 * (可能有没有考虑到的情况 add by xc 20160612
 * @param prefix
 * @param attr
 * @param query
 * @returns {{}}
 */
function destructPrefixQuery(prefix, query) {
	let result = {};
	if(query.hasOwnProperty("$and")) {
		result.$and = query.$and.map(
			(ele, idx) => {
				return destructPrefixQuery(prefix, ele)
			}
		);
	}
	else if(query.hasOwnProperty("$or")) {
		result.$or = query.$or.map(
			(ele, idx) => {
				return destructPrefixQuery(prefix, ele)
			}
		);
	}
	else if(query.hasOwnProperty("$not")) {
		result.$not = destructPrefixQuery(prefix, query.$not)
	}
	else if(query.hasOwnProperty("$nor")) {
		result.$nor = query.$nor.map(
			(ele, idx) => {
				return destructPrefixQuery(prefix, ele)
			}
		);
	}
	else {
		for(let attr in query) {
			const attr2 = (prefix || "") + attr;
			result[attr2] = query[attr];
		}
	}

	return result;
}


function destructPrefixSort(prefix, sort) {
	let result = {};

	for(let attr in sort) {
		const attr2 = (prefix || "") + attr;
		result[attr2] = sort[attr];
	}

	return result;
}


function transformJoinToAggregate(join, aggregation, prefix, projection) {
	let lookup = {
		$lookup: {
			from: join.rel,
			localField: (prefix||"") + join.localColumnName,
			foreignField: join.refColumnName,
			as: (prefix||"") + join.attr
		}
	}

	let unwind = {
		$unwind: {
			path: "$" + (prefix||"") + join.attr,
			preserveNullAndEmptyArrays: true
		}
	};


	aggregation.push(lookup);
	aggregation.push(unwind);

	let node = join.node;
	// 在mongodb中，对被连接的对象的过滤条件被忽略，只传递投影信息
	projection[join.attr] = node.projection;
	let newPrefix = (prefix || "") + join.attr + ".";

	formalizeQuery(node.query);
	let match = {
		$match: destructPrefixQuery(newPrefix, node.query)
	};
	aggregation.push(match);

	if(Object.getOwnPropertyNames(node.sort).length > 0) {
		let sort = {
			$sort : destructPrefixSort(newPrefix, node.sort)
		}
		aggregation.push(sort);
	}

	for(let i in node.joins) {
		transformJoinToAggregate(node.joins[i], aggregation, newPrefix, projection[join.attr]);
	}
}

class Mongodb {
	constructor(settings) {
		this.settings = settings || {};
	}


	connect(url) {
		return new Promise((resolve, reject) => {
			MongoClient.connect(url, (err, db) => {
				if (err) {
					reject(err);
				}
				else {
					this.db = db;
					this.collections = {};
					resolve();
				}
			})
		});
	}

	disconnect() {
		return new Promise((resolve, reject) => {
			this.db.close((err) => {
				this.db = undefined;
				if (err) {
					reject(err);
				}
				else {
					resolve();
				}
			})
		})
	}

	find(name, execTree, indexFrom, count, isCounting) {
		return getCollection.call(this, name)
			.then(
				(collection) => {
					if(isCounting) {
						if(execTree.joins && execTree.joins.length > 1) {
							throw new Error("mongodb的count计算不能跨越多张表");
						}
						return collection.count(execTree.query)
							.then(
								(number) => {
									return Promise.resolve({
										count: number
									});
								}
							)
					}
					else {
						let aggregation = [];
						let projection = execTree.projection;
						formalizeQuery(execTree.query)
						aggregation.push({
							$match: execTree.query
						});
						if(Object.getOwnPropertyNames(execTree.sort).length > 0) {
							let sort = {
								$sort : execTree.sort
							}
							aggregation.push(sort);
						}
						aggregation.push({
							$skip: indexFrom
						});
						aggregation.push({
							$limit: count
						});
						for(let i in execTree.joins) {
							const join = execTree.joins[i];
							transformJoinToAggregate(join, aggregation, "", projection);
						}

						aggregation.push({
							$project: projection
						})

						return collection.aggregate(
							aggregation
						).toArray();
					}
				}
			)
	}

	insert(name, data) {
		return getCollection.call(this, name)
			.then(
				(collection) => {
					if (data instanceof Array) {
						return collection.insertMany(data)
							.then(
								(result) => {
									let rows = result.ops;
									let rows2 = rows.map((ele, index) => {
										return formalizeResult(ele);
									});

									return Promise.resolve(rows2.length > 1 ? rows2: rows2[0]);
								},
								(err) => {
									return Promise.resolve(err);
								}
							);
					}
					else {
						return collection.insertOne(data)
							.then(
								(result) => {
									return Promise.resolve(formalizeResult(result.ops[0]));
								},
								(err) => {
									return Promise.resolve(err);
								}
							);
					}
				},
				(err) => {
					return Promise.reject(err);
				}
			);
	}

	update(name, updatePart, query) {

		return getCollection.call(this, name)
			.then(
				(collection) => {
					formalizeQuery(query);
					return collection.updateMany(query, updatePart)
						.then(
							(results) => {
								return Promise.resolve(results.result.n);
							},
							(err) => {
								return Promise.reject(err);
							}
						);
				},
				(err) => {
					return Promise.reject(err);
				}
			);
	}

	updateOneById(name, updatePart, id) {

		return getCollection.call(this, name)
			.then(
				(collection) => {
					return collection.findOneAndUpdate(
						{
							_id: new ObjectId(id)
						},
						updatePart,
						{
							returnOriginal: false               // 返回后项
						}
					).then(
						(result) => {
							return Promise.resolve(formalizeResult(result.value));
						},
						(err) => {
							return Promise.reject(err);
						}
					)
				},
				(err) => {
					return Promise.reject(err);
				}
			);
	}

	remove(name, query) {
		return getCollection.call(this, name)
			.then(
				(collection) => {
					formalizeQuery(query);
					return collection.remove(
						query
					).then(
						(result) => {
							return Promise.resolve();
						},
						(err) => {
							return Promise.reject(err);
						}
					);
				},
				(err) => {
					return Promise.reject(err);
				}
			);
	}

	removeOneById(name, id) {
		return getCollection.call(this, name)
			.then(
				(collection) => {
					return collection.findOneAndDelete({
							_id: new ObjectId(id)
						}
					).then(
						(result) => {
							return Promise.resolve(formalizeResult(result.value));
						},
						(err) => {
							return Promise.reject(err);
						}
					);
				},
				(err) => {
					return Promise.reject(err);
				}
			);

	}

	createSchema(schema, name) {
		return this.db.createCollection(name, {strict: false})
			.then(
				(collection) => {
					this.collections[name] = collection;

					if (schema.indexes) {
						let promises = [];
						for (let index in schema.indexes) {
							const indexDef = schema.indexes[index];
							let options = indexDef.options || {};
							options.name = index;
							options.w = 1;
							promises.push(new Promise((resolve, reject) => {
								collection.createIndex(indexDef.columns, options, (err, result) => {
									if (err) {
										reject(err);
									}
									else {
										resolve(result);
									}
								});
							}));
						}
						return Promise.all(promises)
							.then(
								(results) => {
									return Promise.resolve();
								},
								(err) => {
									return Promise.reject(err);
								}
							);
					}
					else {
						return Promise.resolve();
					}
				},
				(err) => {
					return Promise.reject(err);
				}
			)
	}

	dropSchema(schema, name) {
		/*return this.db.dropCollection(name)
		 .then(
		 (result) => {
		 this.collections[name] = undefined;
		 return Promise.resolve();
		 },
		 (err) => {
		 if (err.code === 26) {
		 return Promise.resolve();
		 }
		 else {
		 return Promise.reject(err);
		 }
		 }
		 );*/
		return new Promise((resolve, reject) => {
			this.db.collection(name, (err, collection) => {
				if(err) {
					reject(err);
				}
				if(collection) {
					collection.drop()
						.then(
							(result) => {
								resolve();
							},
							(err) => {
								if(err.code === 26) {
									resolve();          // 这里在并发创建和drop表的时候貌似会有一些奇怪的问题  by xc
								}
								else {
									reject(err);
								}
							}
						)
				}
				else {
					resolve();
				}
			})
		});
	}

	getDefaultKeyType() {
		return new Promise(
			(resolve, reject) => {
				resolve({
					type: "string",
					size: 24
				});
			}
		);
	}

	getDefaultKeyName() {
		return new Promise(
			(resolve, reject) => {
				resolve("_id");
			}
		)
	}
}


module.exports = Mongodb;