/**
 * Created by Administrator on 2016/6/1.
 */
"use strict";

const EventEmitter = require("events");
const mysql = require("./drivers/mysql");
const mongodb = require("./drivers/mongodb");
const remote = require("./drivers/remote");

const ObjectId = require("mongodb").ObjectID;

const assert = require("assert");
const merge = require("lodash").merge;
const get = require("lodash").get;
const set = require("lodash").set;
const assign = require("lodash/assign");
const keys = require("lodash/keys");
const omit = require("lodash/omit");
const uniq = require("lodash/uniq");

const constants = require("./constants");
const events = require("./events");

function isSettingTrueStrictly(settings, option) {
    return settings && (settings[option] === true)
}

/**
 * 处理schema定义里的ref语义
 * @param schemas
 */
function formalizeSchemasDefinition(schemas) {
    const connections = this.connections;
    const dataSources = this.dataSources;

    return new Promise(
        (resolve, reject) => {

            let schemasPromises = [];
            for(let schema in schemas) {
                const schemaDef = schemas[schema];
                const connection = connections[schemaDef.source];
                if(!connection) {
                    throw new Error("寻找不到相应的数据源" + schemaDef.source);
                }
                else {
                    schemasPromises.push(
                        connection.getDefaultKeyName(schema)
                            .then(
                                (keyName) => {
                                    let isMainKeyDefined = false;
                                    let mainKeyColumn = keyName;

                                    // 处理所有的reference列
                                    let keyTypePromises = [];
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
                                                let type = dataSource2.getDefaultKeyType(attrDef.ref);
                                                for(let attr2 in schemaReferenced.attributes) {
                                                    const attrDef2 = schemaReferenced.attributes[attr2];
                                                    if(attrDef2.key) {
                                                        if(attrDef2.type === "serial") {
                                                            type = {
                                                                type: "int",
                                                                size: 8
                                                            };
                                                        }
                                                        else {
                                                            type = attrDef2.type;
                                                        }
                                                        attrDef.refColumnName = attr2;
                                                    }
                                                }
                                                let localColumnName = attrDef.localColumnName || (attr + "Id");
                                                attrDef.localColumnName = localColumnName;
                                                attrDef.refColumnName = (attrDef.refColumnName || findKeyColumnName.call(this, attrDef.ref, schemaReferenced));
                                                if(attrDef.refColumnName instanceof Promise) {
                                                    keyTypePromises.push(attrDef.refColumnName
                                                        .then(
                                                            (columnName) => {
                                                                attrDef.refColumnName = columnName;
                                                                return Promise.resolve();
                                                            },
                                                            (err) => {
                                                                return Promise.reject(err);
                                                            }
                                                        )
                                                    );
                                                }

                                                let attrDef3 = Object.assign({}, attrDef);
                                                if(type instanceof Promise) {
                                                    // 异步获取keyType
                                                    keyTypePromises.push(
                                                        type
                                                            .then(
                                                                (keyType) => {
                                                                    if(keyType === "serial") {
                                                                        // 特殊处理一下serial类型，如果是serial则转成bigint
                                                                        keyType = {
                                                                            type: "int",
                                                                            size: 8
                                                                        };
                                                                    }
                                                                    attrDef3.type = keyType;
                                                                    return Promise.resolve();
                                                                },
                                                                (err) => {
                                                                    return Promise.reject(err);
                                                                }
                                                            )
                                                    );
                                                }
                                                else {
                                                    attrDef3.type = type;
                                                }

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
                                        keyTypePromises.push(
                                            connection.getDefaultKeyType(schema)
                                                .then(
                                                    (keyType) => {
                                                        schemaDef.attributes[mainKeyColumn] = {
                                                            key: true,
                                                            type: keyType
                                                        };
                                                        return Promise.resolve();
                                                    },
                                                    (err) => {
                                                        return Promise.reject(err);
                                                    }
                                                )
                                        );
                                    }

                                    if(!isSettingTrueStrictly(dataSources[schemaDef.source].settings, "disableCreateAt")) {
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

                                    if(!isSettingTrueStrictly(dataSources[schemaDef.source].settings, "disableUpdateAt")) {
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

                                    if(!isSettingTrueStrictly(dataSources[schemaDef.source].settings, "disableDeleteAt")) {
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

                                    if(keyTypePromises.length > 0) {
                                        return Promise.all(keyTypePromises);
                                    }
                                    else {
                                        return Promise.resolve();
                                    }
                                },
                                (err) => {
                                    return Promise.reject(err);
                                }
                            )
                    );
                }
            }
            if(schemasPromises.length > 0) {
                return Promise.all(schemasPromises)
                    .then(
                        resolve,
                        reject
                    )
                    .catch(
                        reject
                    );
            }
            else {
                return resolve();
            }
        }
    )
}

function formalizeDataForUpdate(data, schema, type, omitTsColumn) {
    let setPart = data.$set;
    let incPart = data.$inc;

    if (!setPart && !incPart) {
        setPart = data;
    }
    if (setPart) {
        keys(setPart).forEach(
            (attr) => {
                if(!schema.attributes[attr]) {
                    throw new Error("更新的数据拥有非法属性" + attr);
                }

                // 处理reference列
                if(schema.attributes[attr].type === constants.typeReference) {
                    const localColumnName = schema.attributes[attr].localColumnName;
                    const refColumnName = schema.attributes[attr].refColumnName;

                    if(!setPart[localColumnName]) {
                        setPart = assign({}, setPart, {
                            [localColumnName] : setPart[attr] ? setPart[attr][refColumnName] : null
                        });
                    }

                    setPart = omit(setPart, attr);
                }
                else if(schema.attributes[attr].type === "date" || schema.attributes[attr].type === "time") {
                    // 处理Date类型
                    if(setPart[attr] instanceof Date) {
                        setPart = assign({}, setPart, {
                            [attr] : setPart[attr].valueOf()
                        });
                    }
                }
            }
        );
    }
    if (incPart) {
        if (type && type === 'create') {
            throw new Error('创建对象时不能传$inc算子');
        }
        keys(incPart).forEach(
            (attr) => {
                if (!schema.attributes[attr]) {
                    throw new Error("更新的数据拥有非法属性" + attr);
                }
                if (schema.attributes[attr].type === constants.typeReference) {
                    throw new Error(`$inc算子不能支持引用属性${attr}`);
                }
                // todo 判断这个对象类型是否可以增加，暂时先不用
                if (typeof incPart[attr] !== 'number') {
                    throw new Error(`属性${attr}上的$inc算子的值必须是数值，当前是${incPart[attr]}`);
                }
            }
        );
    }

    // 增加相应的时间列
    switch(type) {
        case "create": {
            assert(setPart);
            if (!omitTsColumn) {
                setPart[constants.createAtColumn] = Date.now();
                setPart[constants.updateAtColumn] = Date.now();
            }
            return setPart;
            break;
        }
        case "update": {
            if (!omitTsColumn) {
                if (!setPart) {
                    setPart = {};
                }
                setPart[constants.updateAtColumn] = Date.now();
            }
            const newData = {};
            if (incPart) {
                newData.$inc = incPart;
            }
            if (setPart) {
                newData.$set = setPart;
            }
            return newData;
            break;
        }
        default: {
            assert(false);
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


function formalizeQueryValue(value) {
    if(typeof value === "object") {
        if(value instanceof Date) {
            return value.valueOf();
        }
        else if(value instanceof Array) {
            value = value.map(
                (ele) => {
                    return formalizeQueryValue(ele);
                }
            )
        }
        else {
            for(let i in value) {
                if(i.startsWith("$")) {
                    value[i] = formalizeQueryValue(value[i]);
                }
            }
        }
    }

    return value;
}

function formalizeProjection(schema, projection, ignoreRef, isCounting) {
    if(projection && typeof projection === "object") {
        return projection;
    }
    let proj2 = {};
    for(let i in schema.attributes) {
        if(schema.attributes[i].type === constants.typeReference && !isCounting) {
            // 这里如果去将自己的ref全取出来，在自己ref自己的情况下会造成无限递归
            // 更新，这里上层需求取一层出来
            if(!ignoreRef) {
                proj2[i] = formalizeProjection.call(this, this.schemas[schema.attributes[i].ref], null,  true);
            }
        }
        else {
            proj2[i] = 1;
        }
    }
    return proj2;
}

/**
 * 将查询、投影和排序结合降解成一棵树结构，树中的每个结点是对于一张表的查询、投影和排序，其中的joins数组包含了对子树的连接信息
 * @param name
 * @param projection
 * @param query
 * @param sort
 * @returns {{joins: Array, projection: {}, query: {}}}
 */
function destructSelect(name, projection, query, sort, isCounting) {
    let result = {
        joins: [],
        projection: {},
        query: {},
        sort: {}
    };
    const schema = this.schemas[name];
    projection = formalizeProjection.call(this, schema, projection, isCounting);
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
                let refProjection = assign({}, projection[attr], {
                    [attrDef.refColumnName]: 1
                });
                let join = {
                    rel: attrDef.ref,
                    attr: attr,
                    refColumnName: attrDef.refColumnName,
                    localColumnName: attrDef.localColumnName,
                    node: destructSelect.call(this, schema.attributes[attr].ref, refProjection, query[attr], sort[attr])
                }
                result.joins.push(join);
            }
        }
        else {
            if(projection.hasOwnProperty(attr)) {
                result.projection[attr] = projection[attr];
            }
            if(query.hasOwnProperty(attr)) {
                result.query[attr] = formalizeQueryValue(query[attr]);
            }
            if(sort.hasOwnProperty(attr)) {
                result.sort[attr] = sort[attr];
            }
        }
    }

    // 处理projection、query和sort中的fnCall
    for(let attr in projection) {
        if(attr.toLowerCase().startsWith("$fncall")) {
            result.projection[attr] = projection[attr];
        }
    }
    // sort可能按照函数调用结果的重命名列排序，所以把所有的项都传进去
    for(let attr in sort) {
        if(!result.sort.hasOwnProperty(attr)) {
            if(typeof sort[attr] === "number" || attr.toLowerCase().startsWith("$fncall")) {
                result.sort[attr] = sort[attr];
            }
        }
    }

    function checkQueryIsNoRef(query, attributes) {
        let noRef = true;
        for(let i in query) {
            if(attributes[i]) {
                if(attributes[i].type === "ref") {
                    return false;
                }
            }
            else {
                if(i.startsWith("$")) {
                    query[i].forEach(
                        (ele) => {
                            if(!checkQueryIsNoRef(ele, attributes)) {
                                noRef = false;
                            }
                        }
                    );
                    if(!noRef) {
                        return false;
                    }
                }
            }
        }
        return noRef;
    }

    for(let i in query) {
        if(i.startsWith('$')) {
            if(i === "$and" || i === "$or" || i === "$nor") {
                const values = query[i];
                let legal = true;
                for(let j = 0; j < values.length; j ++){
                    if(!checkQueryIsNoRef(values[j], schema.attributes)) {
                        legal = false;
                        break;
                    }
                }
                if(!legal) {
                    throw new Error("逻辑顶层算子" + i + "只能支持单表级语义");
                }
                else {
                    result.query[i] = query[i];
                }
            }
            else if(i === "$has" || i === "$hasnot") {
                // exists / not exists 查询
                result.query[i] = {
                    name: query[i].name,
                    execTree: destructSelect.call(this, query[i].name, query[i].projection, query[i].query, null)
                };
            }
            else if(i.toLowerCase().startsWith("$fncall")) {
                result.query[i] = query[i];
            }
            else {
                throw new Error("检测到尚未支持的顶层算子: " + i );
            }
        }
    }
    query["#execNode#"] = result;		// 这个指针为subquery未来找到所指向的execNode所使用
    return result;
}

function distributeNode(result, node, name, treeName, path) {
    const schemas = this.schemas;
    let newJoins = [];
    for(let i = 0; i < node.joins.length; i ++) {
        let join = node.joins[i];
        if(schemas[name].source !== schemas[join.rel].source) {
            // 如果子表与本表非同一个源，则将子表的查询剥离
            join.node.referencedBy = treeName;
            join.node.referenceNode = node;
            if(result[join.rel]) {
                // 已经对本表有一个子树了，不能重名
                let alias = join.rel + "_1";
                while(result[alias]) {
                    alias += "_1";
                }
                join.node.relName = join.rel;
                result[alias] = join.node;
            }
            else {
                result[join.rel] = join.node;
            }
            const localColumnName = join.localColumnName;
            let joinInfo = {
                localKeyPath: path + localColumnName,
                localAttrPath: path + join.attr,
                refAttr: join.refColumnName
            };
            join.node.joinInfo = joinInfo;

            // 在子表的查询中加上主键
            join.node.projection = merge({}, join.node.projection, {
                [join.refColumnName]: 1
            });

            delete join.node;
            node.joins

            // 此时要在本表的查询投影中加上子表的外键
            node.projection = merge({}, node.projection, {
                [localColumnName]: 1
            });
            distributeNode.call(this, result, result[join.rel], join.rel, join.rel, "");
        }
        else {
            let path2 = path + join.attr + ".";
            distributeNode.call(this, result, join.node, join.rel, treeName, path2);
            newJoins.push(join);
        }
    }
    node.joins = newJoins;
}

function distributeSelect(name, tree) {
    let result = {
        [name]: tree
    };
    distributeNode.call(this, result, tree, name, name, "");
    return result;
}




function hasOperator(node, op, num) {
    let count = num || 0;
    if(Object.getOwnPropertyNames(node[op]).length > count) {
        return true;
    }
    else {
        if(node.joins.length > 0) {
            for(let i = 0; i < node.joins.length; i ++) {
                if(node.joins[i].node && hasOperator(node.joins[i].node, op, num)) {
                    return true;
                }
            }
        }
        return false;
    }
}

function findKeyColumnName(tblName, schema) {
    for(let i in schema.attributes) {
        if(schema.attributes[i].key) {
            return i;
        }
    }

    return this.connections[schema.source].getDefaultKeyName(tblName);
}

/**
 * 检查一颗查询树中所有的算子是否满足这颗树本身为NULL
 * @param node
 */
function checkRightNodeQuerySatisfyNull(node, root) {
    function checkQuerySatisfied(query) {
        if(!query || Object.getOwnPropertyNames(query).length === 0) {
            return true;
        }

        for(let i in query) {
            if(i === "$and") {
                let j = query[i].find(
                    (ele) => {
                        return (checkQuerySatisfied(ele) === false);
                    }
                );
                if(j) {
                    return false;
                }
            }
            else if(i.startsWith("$")) {
                // 除了and的一切一级算子直接报错
                return false;
            }
            else if(!query[i].hasOwnProperty("$exists") || query[i].$exists !== false) {
                // 如果用户显式地传了右子树的“id in”这样的查询，这里逻辑上会出错，不过这种概率极低
                if(!root || i !== node.joinInfo.refAttr || !query[i].hasOwnProperty("$in") || !(query[i].$in instanceof Array)) {
                    return false;
                }
            }
        }
        return true;
    }
    let query = node.query;
    if(!checkQuerySatisfied(query)) {
        return false;
    }

    if(node.joins.length > 0) {
        for(let i = 0; i < node.joins.length; i ++) {
            if(node.joins[i].node && !checkRightNodeQuerySatisfyNull(node.joins[i].node, false)) {
                return false;
            }
        }
    }

    return true;
}


/**
 * 处理连接的主函数
 * @param forest      查询森林
 * @param me        当前子树名
 * @param result    当前子树查询结果（数组）
 * @returns {*}
 * @description 本函数负责将当前子树的查询结果与父树或子树连接，并在需要的时候发出新的JOIN请求。
 *                  在两种情况下结束，1）当前子树的查询结果为空；2）当前子树是森林中的最后一棵查询子树
 */
function joinNext(forest, me, result) {
    const nodeMe = forest[me];
    let promises = [];
    nodeMe.result = result;
    if(nodeMe.referencedBy) {
        const nodeParent = forest[nodeMe.referencedBy];
        if(nodeParent.result) {
            // 将自己的查询结果和父亲的结果进行join
            const parentResult = nodeParent.result;

            let joinedResult = [];
            parentResult.forEach(
                (ele, idx) => {
                    let joinLocalValue = get(ele, nodeMe.joinInfo.localKeyPath);
                    let i;
                    for(i = 0; i < result.length; i ++) {
                        let joinRefValue = result[i][nodeMe.joinInfo.refAttr];
                        // 这里要处理mongodb的ObjectId类型，不是一个很好的方案  by xc
                        if(joinRefValue instanceof ObjectId) {
                            joinRefValue = joinRefValue.toString();
                        }
                        if(joinLocalValue instanceof ObjectId) {
                            joinLocalValue = joinLocalValue.toString();
                        }
                        if(joinRefValue === joinLocalValue) {
                            set(ele, nodeMe.joinInfo.localAttrPath, result[i]);
                            joinedResult.push(ele);
                            break;
                        }
                    }

                    if(i === result.length) {
                        // 说明子表中没有相应的行，置undefined还是null?
                        /**
                         * 左连逻辑：
                         *      正确的左连逻辑应该是：根据键值找到右表上的项进行连接，如果没有则为NULL，再将右表上的过滤条件下降到右项上进行过滤（此时如果这些过滤条件是IS NULL，是可以通过的）
                         *
                         *      代码运行到这里，连接键值和右项上的过滤条件已经全部下降到右表的查询中进行了过滤，所以检查右表中的查询算子，如果全部是：
                         *      1）is null
                         *      或2）is not deleted
                         *      则返回此项，否则不返回此项
                         *      by xc 20160911
                         */
                        if(checkRightNodeQuerySatisfyNull(nodeMe, true)) {
                            set(ele, nodeMe.joinInfo.localAttrPath, null);
                            joinedResult.push(ele);
                        }
                    }
                }
            );
            nodeParent.result = joinedResult;
        }
        else {
            if(result.length > 0) {
                // 将自己的查询结果转换成in条件，进行父亲的查询
                let referenceNode = nodeMe.referenceNode;
                let joinLocalValues = result.map(
                    (ele, idx) => {
                        return ele[nodeMe.joinInfo.refAttr];
                    }
                );
                referenceNode.query = merge({}, referenceNode.query, {
                    [nodeMe.joinInfo.localKeyPath] : {
                        $in: joinLocalValues
                    }
                });
                let name = nodeParent.relName || nodeMe.referencedBy;
                let connection = this.connections[this.schemas[name].source];

                promises.push(
                    connection.find(name, nodeParent, 0, result.length)
                        .then(
                            (result2) => {
                                return joinNext.call(this, forest, nodeMe.referencedBy, result2);
                            },
                            (err) => {
                                return Promise.reject(err);
                            }
                        )
                );
            }
            else {
                nodeParent.result = [];
            }
        }
    }

    let completed = true;
    let root ;
    for (let i in forest) {
        const node = forest[i];

        if(!node.result) {
            completed = false;
        }
        if(!node.referencedBy) {
            root = node;
        }
        else if(node.referencedBy === me) {
            const nodeSon = node;
            if(nodeSon.result) {
                // 将子结点的结果与自己进行join
                // fixed 这里由于要保持order的顺序，只能由子向父倒查寻。由于在对父亲进行查询时规定了count不大于子查询结果的count,因此这里不会有过量的问题
                const resultSon = nodeSon.result;

                let newResult = [];
                resultSon.forEach(
                    (ele, idx) => {
                        let joinRefValue = ele[nodeSon.joinInfo.refAttr];
                        let i;
                        for(i = 0; i < result.length; i ++) {
                            let joinLocalValue = get(result[i], nodeSon.joinInfo.localKeyPath);
                            // 这里要处理mongodb的ObjectId类型，不是一个很好的方案  by xc
                            if(joinRefValue instanceof ObjectId) {
                                joinRefValue = joinRefValue.toString();
                            }
                            if(joinLocalValue instanceof ObjectId) {
                                joinLocalValue = joinLocalValue.toString();
                            }
                            if(joinRefValue === joinLocalValue) {
                                set(result[i], nodeSon.joinInfo.localAttrPath, ele);
                                newResult.push(result[i]);
                                break;
                            }

                        }
                    }
                );
                nodeMe.result = newResult;

                /*result.forEach(
                 (ele, idx) => {
                 let joinLocalValue = get(ele, nodeSon.joinInfo.localKeyPath);
                 let i;
                 for(i = 0; i < resultSon.length; i ++) {
                 let joinRefValue = resultSon[i][nodeSon.joinInfo.refAttr];
                 // 这里要处理mongodb的ObjectId类型，不是一个很好的方案  by xc
                 if(joinRefValue instanceof ObjectId) {
                 joinRefValue = joinRefValue.toString();
                 }
                 if(joinLocalValue instanceof ObjectId) {
                 joinLocalValue = joinLocalValue.toString();
                 }
                 if(joinRefValue === joinLocalValue) {
                 set(ele, nodeSon.joinInfo.localAttrPath, resultSon[i]);
                 break;
                 }
                 }

                 if(i === result.length) {
                 // 说明子表中没有相应的行，置undefined还是null?
                 }
                 }
                 )*/

            }
            else {
                if(result.length > 0) {
                    // 将自己的查询结果转换成in形式，进行子孙的查询
                    /* let joinLocalValues = [];
                     result.forEach(
                     (ele) => {
                     const localValue = get(ele, nodeSon.joinInfo.localKeyPath);
                     if(localValue){
                     joinLocalValues.push(localValue);
                     }
                     }
                     );*/
                    let joinLocalValues = uniq(result.map(
                        (ele, idx) => {
                            return get(ele, nodeSon.joinInfo.localKeyPath);
                        }
                    ));

                    /*// 又要处理 mongodb _id的特殊情况
                     if(nodeSon.joinInfo.refAttr === "_id") {
                     try{
                     let joinLocalValues2 = joinLocalValues.map(
                     (ele) => {
                     return new ObjectId(ele);
                     }
                     );
                     joinLocalValues = joinLocalValues2;
                     }
                     catch(e) {
                     console.log("跨库连接出现了_id关键字，但相应的值未能成功转化成ObjectId类型")
                     }
                     }*/
                    let query = {
                        $and: [
                            nodeSon.query,
                            {
                                [nodeSon.joinInfo.refAttr]: {
                                    $in: joinLocalValues
                                }
                            }
                        ]
                    };
                    nodeSon.query = query;
                    let name = (nodeSon.relName || i);
                    let connection = this.connections[this.schemas[name].source];

                    promises.push(
                        connection.find(name, nodeSon, 0, result.length)
                            .then(
                                (result3) => {
                                    return joinNext.call(this, forest, i, result3);
                                },
                                (err) => {
                                    return Promise.reject(err);
                                }
                            )
                    );
                }
                else {
                    nodeSon.result = [];
                }
            }
        }
    }

    // 如果所有的查询都完成，则返回root的结果
    if(completed || promises.length === 0) {
        return Promise.resolve(root.result);
    }
    else {
        return Promise.all(promises);
    }
}


/**
 * 执行查询森林
 * @param forest
 * @param indexFrom
 * @param count
 * @returns {Promise.<TResult>}
 */
function execOverSourceQuery(name, forest, indexFrom, count, txn) {
    for(let i in forest) {
        const tree = forest[i];
        if(i !== name && hasOperator(tree, "sort")) {
            throw new Error("跨源查询的排序算子必须在主表上");
        }
    }

    let firstRel = forest[name];
    let root = forest[name];

    const schema = this.schemas[name];
    const connection = this.connections[schema.source];
    const txn2 = (txn && txn.source === schema.source) ? txn.txn : undefined;

    return connection.find(name, firstRel, indexFrom, count, false, txn2)
        .then(
            (result) => {
                return joinNext.call(this, forest, name, result)
                    .then(
                        () => {
                            return Promise.resolve(root.result);
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



function getRidOfResult(result, projection, name) {
    const schemas = this.schemas;
    const schema = schemas[name];

    projection = formalizeProjection.call(this, schema, projection);
    for(let attr in result) {
        if(!projection[attr]) {
            // delete result[attr];
            continue;
        }
        else {
            switch(schema.attributes[attr].type) {
                case "ref" : {
                    getRidOfResult.call(this, result[attr], projection[attr], schema.attributes[attr].ref);
                    break;
                }
                /* case "date":
                 case "time": {
                 result[attr] = new Date(result[attr]);
                 break;
                 }*/
                case "array":
                case "object": {
                    if(typeof result[attr] === "string") {
                        // 从mysql中获取的object类型应该是string
                        result[attr] = JSON.parse(result[attr]);
                        break;
                    }
                }
                case "bool":
                case "boolean": {
                    switch(typeof result[attr]) {
                        case "boolean": {
                            result[attr] =  result[attr];
                            break;
                        }
                        case "number": {
                            result[attr] =  (result[attr] === 0) ? false : true;
                            break;
                        }
                        case "string": {
                            result[attr] =  (result[attr] === "0" || result[attr] === "false") ? false: true;
                            break;
                        }
                    }
                }
                default:
                    break;
            }
        }
    }
}


class DataAccess extends EventEmitter{

    checkTransactionValid(txn) {
        if (!txn || !txn.hasOwnProperty('txn') || !txn.hasOwnProperty('source') ||
            !txn.hasOwnProperty('state')) {
            throw new Error('必须传入有效的txn结构');
        }
        if (txn.state !== 'active') {
            throw new Error('事务处于非活跃状态');
        }
    }

    constructor() {
        super();
        this.drivers = {};
        this.drivers.mysql = mysql;
        this.drivers.mongodb = mongodb;
        this.drivers.remote = remote;
        this.txnCount = 0;
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
        // 为了不改变schema本身，这里创造一个新的对象
        this.schemas = JSON.parse(JSON.stringify(schemas));

        // 将schema处理成为内部格式
        return formalizeSchemasDefinition.call(this, this.schemas)
            .then(
                () => {
                    for(let conn in this.connections) {
                        if(this.connections[conn].setSchemas && typeof this.connections[conn].setSchemas === "function") {
                            this.connections[conn].setSchemas(this.schemas);
                        }
                    }

                    return Promise.resolve();
                }
            )

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
                // 这里增加需求，可以指定某张表在Drop时不删除，可能有问题  by xc!
                if(!schemaDef.static) {
                    promises.push(connection.dropSchema(schemaDef, schema));
                }
            }
        }
        return Promise.all(promises);
    }

    insert(name, data, txn) {
        if (txn) {
            this.checkTransactionValid(txn);
        }
        let schema = this.schemas[name];
        const connection = this.connections[schema.source];

        if (typeof data !== "object") {
            throw new Error("插入的数据必须是object类型");
        }

        let omitTsColumn = true;
        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableCreateAt")){
            omitTsColumn = false;
        }
        let data2;
        if (!(data instanceof Array)) {
            data2 = [formalizeDataForUpdate(assign({}, data), schema, 'create', omitTsColumn)];
        }
        else {
            data2 = data.map(
                (ele) => formalizeDataForUpdate(assign({}, ele), schema, 'create', omitTsColumn)
            );
        }

        return connection.insert(name, data2, schema, txn && txn.txn);
    }

    find(name, projection, query, sort, indexFrom, count, txn) {
        if(!name || !this.schemas[name]) {
            throw new Error("查询必须输入有效表名");
        }
        if(indexFrom === undefined || !count) {
            throw new Error("查询列表必须带indexFrom和count参数");
        }
        if (txn) {
            this.checkTransactionValid(txn);
        }
        let execTree = destructSelect.call(this, name, projection, query, sort);

        return this.findByExecTreeDirectly(name, execTree, indexFrom, count, false, txn)
            .then(
                (result) => {
                    assert(result instanceof Array);
                    result.forEach(
                        (ele, idx) => {
                            getRidOfResult.call(this, ele, projection, name);
                        }
                    );
                    return Promise.resolve(result);
                }
            );
    }

    count(name, query, txn) {
        if(!name || !this.schemas[name]) {
            throw new Error("查询必须输入有效表名");
        }
        if (txn) {
            this.checkTransactionValid(txn);
        }

        let execTree = destructSelect.call(this, name, null, query, null, true);
        return this.findByExecTreeDirectly(name, execTree, undefined, undefined, true, txn);

    }

    findOneById(name, projection, id, txn) {
        return this.findById(name, projection, id, txn);
    }

    findById(name, projection, id, txn) {
        if(!name || !this.schemas[name]) {
            throw new Error("查询必须输入有效表名");
        }

        if(typeof id !== "number" && typeof id !== "string" && ! (id instanceof ObjectId)) {
            throw new Error("查询必须输入有效id")
        }

        if (txn) {
            this.checkTransactionValid(txn);
        }

        let schema = this.schemas[name];
        let query = {};
        const connection = this.connections[schema.source];

        const pKeyColumn = findKeyColumnName.call(this, name, schema);
        assert (!(pKeyColumn instanceof Promise));            // 在运行中去获取主键应该不需要等待
        query[pKeyColumn] = id;

        let execTree = destructSelect.call(this, name, projection, query);
        return this.findByExecTreeDirectly(name, execTree, 0, 1, null, txn)
            .then(
                (result) => {
                    switch (result.length) {
                        case 0: {
                            return Promise.resolve(null);
                        }
                        case 1: {
                            let row = result[0];
                            getRidOfResult.call(this, row, projection, name);
                            return Promise.resolve(row);
                        }
                        case 2: {
                            return Promise.reject(new Error("基于键值的查询返回了一个以上的结果"));
                        }
                    }
                },
                (err) => {
                    return Promise.reject(err);
                }
            );
    }


    findByExecTreeDirectly(name, execTree, indexFrom, count, isCounting, txn) {
        let execForest = distributeSelect.call(this, name, execTree);

        let trees = Object.getOwnPropertyNames(execForest);
        if(trees.length > 1) {
            // 如果查询跨越了数据源，则必须要带sort条件，否则无法进行查询
            if(isCounting) {
                throw new Error("当前不支持跨源的count查询");
            }

            return execOverSourceQuery.call(this, name, execForest, indexFrom, count, txn);
        }
        else {
            // 单源的查询直接PUSH给相应的数据源
            let schema = this.schemas[name];
            const connection = this.connections[schema.source];
            const txn2 = (txn && txn.source === schema.source) ? txn.txn : undefined;

            return connection.find(name, execTree, indexFrom, count, isCounting, txn2);
        }
    }

    update(name, data, query, txn) {
        if (txn) {
            this.checkTransactionValid(txn);
        }
        let schema = this.schemas[name];
        query = query || {};
        const connection = this.connections[schema.source];

        let omitTsColumn = true;
        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableUpdateAt")){
            omitTsColumn = false;
        }
        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableDeleteAt")){
            query[constants.deleteAtColumn] = {
                $exists: false
            };
        }

        const data2 = formalizeDataForUpdate(data || {}, schema, 'update', omitTsColumn);
        transformDateTypeInQuery(query);

        return connection.update(name, data2, query, txn && txn.txn);
    }

    updateOneById(name, data, id, txn) {
        if(typeof id !== "number" && typeof id !== "string" && ! (id instanceof ObjectId)) {
            throw new Error("查询必须输入有效id")
        }
        if (txn) {
            this.checkTransactionValid(txn);
        }
        let schema = this.schemas[name];
        const connection = this.connections[schema.source];

        let omitTsColumn = true;
        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableUpdateAt")){
            omitTsColumn = false;
        }

        const data2 = formalizeDataForUpdate(data, schema, 'update', omitTsColumn);

        return connection.updateOneById(name, data2, id, schema, txn && txn.txn);

    }

    remove(name, query, txn) {
        if (txn) {
            this.checkTransactionValid(txn);
        }
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
            return connection.update(name, data, query, txn && txn.txn);
        }
        else {
            return connection.remove(name, query, txn && txn.txn);
        }
    }

    removeOneById(name, id, txn) {
        if(typeof id !== "number" && typeof id !== "string" && ! (id instanceof ObjectId)) {
            throw new Error("查询必须输入有效id")
        }
        if (txn) {
            this.checkTransactionValid(txn);
        }
        let schema = this.schemas[name];
        const connection = this.connections[schema.source];

        if(!isSettingTrueStrictly(this.dataSources[schema.source].settings, "disableDeleteAt")){
            let data = {
                $set: {
                    [constants.deleteAtColumn]: Date.now()
                }
            };
            return connection.updateOneById(name, data, id, schema, txn && txn.txn);
        }
        else {
            return connection.removeOneById(name, id, schema, txn && txn.txn);
        }
    }


    getSource(sourceName) {
        return this.connections[sourceName];
    }

    getKeyName(tblName) {
        let keyName = findKeyColumnName.call(this, tblName, this.schemas[tblName]);
        assert (!(keyName instanceof Promise));            // 在运行中去获取主键应该不需要等待
        return keyName;
    }

    getKeyType(tblName) {
        let columnName = this.getKeyName(tblName);
        assert (!(columnName instanceof Promise));            // 在运行中去获取主键应该不需要等待
        return this.schemas[tblName].attributes[columnName].type;
    }

    /**
     * 开始一个事务
     * @param source
     * @param option {
     *                  isolationLevel: ' SERIALIZABLE',
     *                  extras: " READ ONLY"
     *              }
     * @returns {*}
     */
    startTransaction(source, option) {
        const connection = this.connections[source];
        if(connection && connection.startTransaction && typeof connection.startTransaction === "function") {
            return connection.startTransaction(option)
                .then(
                    (txn) => {
                        this.txnCount ++;
                        const transaction = {
                            txn,
                            source,
                            state: 'active'
                        };
                        return Promise.resolve(transaction);
                    }
                )
        }
        else {
            throw new Error("源" + source + "不存在，或者不支持事务操作");
        }
    }

    /**
     * 结束一个事务
     * @param txn
     * @param option {
     *                  isolationLevel: ' REPEATABLE READ'
     *              }
     * @returns {*}
     */
    commitTransaction(txn, option) {
        this.checkTransactionValid(txn);
        const connection = this.connections[txn.source];
        if(connection && connection.commmitTransaction && typeof connection.commmitTransaction === "function") {
            return connection.commmitTransaction(txn.txn, option)
                .then(
                    () => {
                        this.txnCount --;
                        txn.state = 'committed';
                        this.emit('txnCommitted', txn);
                    }
                )
        }
        else {
            throw new Error("未发现活跃事务，或者不支持事务操作");
        }
    }

    /**
     * 回滚一个事务
     * @param txn
     * @param option {
     *                  isolationLevel: ' REPEATABLE READ'
     *              }
     * @returns {*}
     */
    rollbackTransaction(txn, option) {
        this.checkTransactionValid(txn);
        this.txnCount --;
        const connection = this.connections[txn.source];
        if(connection && connection.rollbackTransaction && typeof connection.rollbackTransaction === "function") {
            return connection.rollbackTransaction(txn.txn, option)
                .then(
                    () => {
                        txn.state = 'rollbacked';
                        this.emit('txnRollbacked', txn);
                    }
                )
        }
        else {
            throw new Error("未发现活跃事务，或者不支持事务操作");
        }
    }

    get constants(){
        return constants;
    }

    get events() {
        return events;
    }

};


// const dataAccess = new DataAccess();






module.exports = DataAccess;