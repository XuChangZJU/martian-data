/**
 * Created by Administrator on 2016/6/1.
 */
"use strict";
var orm = require('orm');
const mysql = require("./mysql");
const mongodb = require("./mongodb");
const merge = require("lodash").merge;

const constants = require("./constants");

function isSettingTrueStrictly(settings, option) {
    return settings && (settings[option] === true)
}

/**
 * 处理schema定义里的ref语义
 * @param schemas
 */
function formalizeSchemasDefinition(schemas) {
    for(let schema in schemas) {
        const schemaDef = schemas[schema];
        const connection = this.connections[schemaDef.source];
        if(!connection) {
            throw new Error("寻找不到相应的数据源" + schemaDef.source);
        }
        else {
            let isMainKeyDefined = false;
            let mainKeyColumn = connection.getDefaultKeyName();
            // 处理所有的reference列
            for(let attr in schemaDef.attributes) {
                const attrDef = schemaDef.attributes[attr];

                if(attr === mainKeyColumn) {
                    throw new Error("请不要使用默认的主键名" + attr);
                }

                if(attrDef.type === constants.typeReference) {
                    const schemaReferenced = schemas[attrDef.ref];
                    if(!schemaReferenced) {
                        throw new Error("表"+ schema + "定义中的" + attr + "列引用了不存在的表" + attrDef.ref);
                    }
                    else {
                        // 寻找被引用表是否有显式定义主键
                        const dataSource2 = this.connections[schemaReferenced.source];
                        let type = dataSource2.getDefaultKeyType();
                        for(let attr2 in schemaReferenced.attributes) {
                            const attrDef2 = schemaReferenced.attributes[attr2];
                            if(attrDef2.key) {
                                type = attrDef2.type;
                                attrDef.refColumnName = attr2;
                            }
                        }
                        let localColumnName = attrDef.localColumnName || (attr + "Id");
                        attrDef.localColumnName = localColumnName;
                        attrDef.refColumnName = (attrDef.refColumnName || dataSource2.getDefaultKeyName());

                        let attrDef3 = Object.assign({}, attrDef);
                        attrDef3.type = type;

                        schemaDef.attributes[localColumnName] = attrDef3;

                        // 外键上默认要建索引
                        if(!attrDef3.unique && (attrDef3.autoIndexed !== false)) {
                            const indexName = "index_" + localColumnName;
                            const columns = {};
                            columns[localColumnName] = 1;
                            if(schemaDef.indexes) {
                                schemaDef.indexes[indexName] = {
                                    columns: columns
                                }
                            }
                            else {
                                schemaDef.indexes = {
                                    [indexName]: {
                                        columns: columns
                                    }
                                }
                            }
                        }
                    }
                }
                else if(attrDef.key) {
                    // 如果有显式定义主键，则不用增加主键列
                    isMainKeyDefined = true;
                }
            }

            if(!isMainKeyDefined) {
                schemaDef.attributes[mainKeyColumn] = {
                    key: true,
                    type: "serial"
                };
            }

            if(!isSettingTrueStrictly(this.dataSources[schemaDef.source].settings, "disableCreateAt")) {
                // 增加_createAt_
                schemaDef.attributes[constants.createAtColumn] = {
                    type: "date",
                    required: true
                };
                let indexes = merge({}, schemaDef.indexes, {
                    index_createAt: {
                        columns: {
                            [constants.createAtColumn]: 1
                        }
                    }
                });
                schemaDef.indexes = indexes;
            }

            if(!isSettingTrueStrictly(this.dataSources[schemaDef.source].settings, "disableUpdateAt")) {
                // 增加_updateAt_
                schemaDef.attributes[constants.updateAtColumn] = {
                    type: "date"
                };
                let indexes = merge({}, schemaDef.indexes, {
                    index_updateAt: {
                        columns: {
                            [constants.updateAtColumn]: 1
                        }
                    }
                });
                schemaDef.indexes = indexes;
            }

            if(!isSettingTrueStrictly(this.dataSources[schemaDef.source].settings, "disableDeleteAt")) {
                // 增加_deleteAt_
                schemaDef.attributes[constants.deleteAtColumn] = {
                    type: "date"
                };
                let indexes = merge({}, schemaDef.indexes, {
                    index_deleteAt: {
                        columns: {
                            [constants.deleteAtColumn]: 1
                        }
                    }
                });
                schemaDef.indexes = indexes;
            }


        }
    }
}

function formalizeDataForUpdate(data, schema, type) {
    for(let attr in data) {
        if(!schema.attributes[attr]) {
            throw new Error("更新的数据拥有非法属性" + attr);
        }

        // 处理reference列
        if(schema.attributes[attr].type === constants.typeReference) {
            const localColumnName = schema.attributes[attr].localColumnName;
            const refColumnName = schema.attributes[attr].refColumnName;

            if(!data[localColumnName]) {
                data[localColumnName] = data[attr][refColumnName];
            }

            delete data[attr];
        }
        else if(schema.attributes[attr].type === "date" || schema.attributes[attr].type === "time") {
            // 处理Date类型
            if(data[attr] instanceof Date) {
                data[attr] = data[attr].valueOf();
            }
        }


        // 增加相应的时间列
        if(type) {
            switch(type) {
                case "create": {
                    data[constants.createAtColumn] = Date.now();
                    break;
                }
                case "update": {
                    data[constants.updateAtColumn] = Date.now();
                    break;
                }
            }
        }
    }
}

function transformDateTypeInQuery(query) {
    for(let attr in query) {
        const value = query[attr];
        if(typeof value === "object") {
            if(value instanceof Date) {
                query[attr] = value.valueOf();
            }
            else {
                transformDateTypeInQuery(query[attr]);
            }
        }
    }
}


/**
 * 将查询和投影结合降解成一棵树结构
 * @param name
 * @param projection
 * @param query
 * @param sort
 * @returns {{joins: Array, projection: {}, query: {}}}
 */
function destructSelect(name, projection, query, sort) {
    let result = {
        joins: [],
        projection: {},
        query: {},
        sort: {}
    };
    const schema = this.schemas[name];
    projection = projection || {};
    query = query || {};
    sort = sort || {};



    // 选取的数据要过滤掉delete的行
    const settings = this.dataSources[schema.source].settings;
    if(!settings || ! settings.disableDeleteAt) {
        if(!query[constants.deleteAtColumn]) {
            query[constants.deleteAtColumn] = {
                $exists: false
            }
        }
    }

    for(let attr in schema.attributes) {
        const attrDef = schema.attributes[attr];
        if(attrDef.type === constants.typeReference) {
            if(projection[attr] || query[attr] || sort[attr]) {
                let join = {
                    rel: attrDef.ref,
                    attr: attr,
                    refColumnName: attrDef.refColumnName,
                    localColumnName: attrDef.localColumnName,
                    node: destructSelect.call(this, schema.attributes[attr].ref, projection[attr], query[attr], sort[attr])
                }
                result.joins.push(join);
            }
        }
        else {
            if(projection[attr]) {
                result.projection[attr] = projection[attr];
            }
            if(query[attr]) {
                result.query[attr] = query[attr];
            }
            if(sort[attr]) {
                result.sort[attr] = sort[attr];
            }
        }
    }

    return result;
}

function distributeNode(result, node, name, treeName) {
    const schemas = this.schemas;
    for(let i = 0; i < node.joins.length; i ++) {
        let join = node.joins[i];
        if(schemas[name].source !== schemas[join.rel].source) {
            // 如果子表与本表非同一个源，则将子表的查询剥离
            join.node.referencedBy = treeName;
            result[join.rel] = join.node;
            delete join.node;

            // 此时要在本表的查询投影中加上子表的主键
            const localColumnName = schemas[name].attributes[join.attr].localColumnName;
            node.projection = merge({}, node.projection, {
                [localColumnName]: 1
            });
            distributeNode.call(this, result, result[join.rel], join.rel, join.rel);
        }
        else {
            distributeNode.call(this, result, join.node, join.rel, treeName);
        }
    }
}

function distributeSelect(name, tree) {
    let result = {
        [name]: tree
    };
    distributeNode.call(this, result, tree, name, name);
    return result;
}

class DataAccess {

    constructor() {
        this.drivers = {};
        this.drivers.mysql = mysql;
        this.drivers.mongodb = mongodb;
    }

    connect(dataSources) {
        this.connections = {};
        this.dataSources = dataSources;

        // 连接上所有的数据库
        let promises = [];
        for(let name in dataSources) {
            const dsItem = dataSources[name];

            promises.push(new Promise(
                (resolve, reject) => {
                    const driver = this.drivers[dsItem.type];
                    if(!driver) {
                        throw new Error("尚不支持的数据源类型" + dsItem.type);
                    }
                    const instance = new driver(dsItem.settings);
                    instance.connect(dsItem.url)
                        .then(
                            () => {
                                this.connections[name] = instance;
                                resolve();
                            },
                            (err) => {
                                reject(err);
                            }
                        );
                }
            ));
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

    disconnect() {
        let promises = [];
        for(let i in this.connections) {
            const instance = this.connections[i];
            promises.push(
                instance.disconnect()
            );
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

    setSchemas(schemas) {
        this.schemas = Object.assign({}, schemas);

        // 将schema处理成为内部格式
        formalizeSchemasDefinition.call(this, this.schemas);
    }

    createSchemas() {
        let promises = [];

        for(let schema in this.schemas) {
            const schemaDef = this.schemas[schema];
            const connection = this.connections[schemaDef.source];
            if(!connection) {
                throw new Error("寻找不到相应的数据源" + schemaDef.source);
            }
            else {

                promises.push(connection.createSchema(schemaDef, schema));
            }
        }
        return Promise.all(promises);
    }


    dropSchemas() {
        let promises = [];
        for(let schema in this.schemas) {
            const schemaDef = this.schemas[schema];
            const connection = this.connections[schemaDef.source];
            if(!connection) {
                throw new Error("寻找不到相应的数据源" + schemaDef.source);
            }
            else {
                promises.push(connection.dropSchema(schemaDef, schema));
            }
        }
        return Promise.all(promises);
    }

    insert(name, data) {
        let schema = this.schemas[name];
        const connection = this.connections[schema.source];

        if(data instanceof Array) {
            throw new Error("暂时不支持批量插入");
        }
        else {
            let data2 = Object.assign({}, data);
            let create;
            if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableCreateAt")){
                create = "create";
            }
            formalizeDataForUpdate(data2, schema, create);
            return connection.insert(name, data2, schema);
        }
    }

    find(name, projection, query, sort, indexFrom, count) {
        if(!name || !this.schemas[name]) {
            throw new Error("查询必须输入有效表名");
        }
        if(indexFrom === undefined || !count) {
            throw new Error("查询列表必须带indexFrom和count参数");
        }
        let execTree = destructSelect.call(this, name, projection, query, sort);
        let execForest = distributeSelect.call(this, name, execTree);

        let trees = Object.getOwnPropertyNames(execForest);
        if(trees.length > 1) {
            // 如果查询跨越了数据源，则必须要带sort条件，否则无法进行查询
            if(!sort) {
                throw new Error("跨越源的列表查询必须要带上sort条件");
            }
            if(indexFrom !== 0) {
                throw new Error("跨越源的列表查询的indexFrom参数只能为零，通过orderBy属性来实现分页");
            }
            throw new Error("暂时还不支持跨源查询");
        }
        else {
            // 单源的查询直接PUSH给相应的数据源
            let schema = this.schemas[name];
            const connection = this.connections[schema.source];

            return connection.find(name, execTree, 0, 10);

        }

    }

    findById(name, projection, id) {
        if(!name || !this.schemas[name]) {
            throw new Error("查询必须输入有效表名");
        }

        let schema = this.schemas[name];
        let query = {}
        const connection = this.connections[schema.source];
        if(connection.findById && typeof connection.findById === "function") {

        }
        else {
            const pKeyColumn = connection.getDefaultKeyName();
            query[pKeyColumn] = id;
        }

        let execTree = destructSelect.call(this, name, projection, query);
        let execForest = distributeSelect.call(this, name, execTree);

        let trees = Object.getOwnPropertyNames(execForest);
        if(trees.length > 1) {
            throw new Error("暂时还不支持跨源查询");
        }
        else {
            // 单源的查询直接PUSH给相应的数据源

            if(connection.findById && typeof connection.findById === "function") {
                return connection.findById(name, execTree, id);
            }
            else {
                return connection.find(name, execTree, 0, 1)
                    .then(
                        (result) => {
                            switch (result.length) {
                                case 0: {
                                    return Promise.resolve(null);
                                }
                                case 1: {
                                    return Promise.resolve(result[0]);
                                }
                                case 2: {
                                    return Promise.reject(new Error("基于键值的查询返回了一个以上的结果"));
                                }
                            }
                        }
                    )
            }
        }
    }

    update(name, data, query) {
        let schema = this.schemas[name];
        query = query || {};
        const connection = this.connections[schema.source];

        let data2 = Object.assign({}, data);
        let update;
        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableUpdateAt")){
            update = "update";
        }
        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableDeleteAt")){
            query[constants.deleteAtColumn] = {
                $exists: false
            };
        }

        formalizeDataForUpdate(data2.$set || {}, schema, update);
        transformDateTypeInQuery(query);

        return connection.update(name, data2, query);
    }

    updateOneById(name, data, id) {
        let schema = this.schemas[name];
        const connection = this.connections[schema.source];

        let data2 = Object.assign({}, data);
        let update;
        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableUpdateAt")){
            update = "update";
        }

        formalizeDataForUpdate(data2.$set || {}, schema, update);

        return connection.updateOneById(name, data2, id, schema);
    }

    remove(name, query) {
        let schema = this.schemas[name];
        query = query || {};
        const connection = this.connections[schema.source];
        transformDateTypeInQuery(query);

        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableDeleteAt")){
            let data = {
                $set: {
                    [constants.deleteAtColumn]: Date.now()
                }
            };

            query[constants.deleteAtColumn] = {
                $exists: false
            };
            return connection.update(name, data, query);
        }
        else {
            return connection.remove(name, query);
        }
    }

    removeOneById(name, id) {
        let schema = this.schemas[name];
        const connection = this.connections[schema.source];

        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableDeleteAt")){
            let data = {
                $set: {
                    [constants.deleteAtColumn]: Date.now()
                }
            };
            return connection.updateOneById(name, data, id, schema);
        }
        else {
            return connection.removeOneById(name, id);
        }
    }


    getSource(sourceName) {
        return this.connections[sourceName];
    }

};


const dataAccess = new DataAccess();






module.exports = dataAccess;