/**
 * Created by Administrator on 2016/6/4.
 */
"use strict";

const MongoClient = require('mongodb').MongoClient;
const ObjectId = require("mongodb").ObjectID;
const constants = require("./constants");
const merge = require("lodash").merge;


/**
 * 从mongo返回的数据，一定要把_id先强制转换成id，保持全局的统一
 * @param data
 */
function formalizeResult(data) {
    let data2 = Object.assign({}, data);
    if(data2._id instanceof ObjectId) {
        data2.id = data2._id.toString();
        delete data2._id;
        return data2;
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


    findById(name, execTree, id) {

    }

    find(name, execTree, indexFrom, count) {
        
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

                                    return Promise.resolve(rows2);
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
        return this.db.createCollection(name, {strict: true})
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
        return {
            type: "string",
            size: 24
        };
    }

    getDefaultKeyName() {
        return "id";
    }
}


module.exports = Mongodb;