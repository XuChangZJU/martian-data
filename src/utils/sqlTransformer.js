/**
 * Created by Administrator on 2016/6/6.
 */
"use strict";
const assert = require("assert");
const util = require("util");
const ObjectId = require("mongodb").ObjectID;

const assign = require("lodash/assign");
const keys = require("lodash/keys");


function convertValueToSQLFormat(value) {
    if (value === null || value === undefined) {
        return "NULL";
    }
    switch (typeof value) {
        case "number":
        case "boolean":
        {
            return new String(value);
        }
        case "string":
        {
            return "'".concat(value).concat("'");
        }
        case "object":
        {
            if (value instanceof Date) {
                return new String(value.valueOf())
            }
            else if (value instanceof ObjectId) {
                return "'".concat(value.toString()).concat("'");
            }
            else if (value.hasOwnProperty("$ref")) {
                /**
                 *    处理一种特殊情况，在$has这样的subquery中，用
                 *        {
			 *			$ref: Object
			 *			$attr: "id"
			 *		}
                 *    这样的格式来传属性
                 */
                assert(Object.getOwnPropertyNames(value).length === 2 && value.hasOwnProperty("$attr"));
                let result = "`" + value["$ref"]["#execNode#"]["alias"] + "`";
                result += ".";
                result += value["$attr"];
                return result;
            }
            else {
                return "'".concat(JSON.stringify(value)).concat("'");
            }
        }
        default:
            throw new Error("无法转换的数据值" + value);
    }
}

function convertFnCallToSQLFormat(alias, fnCall, projectionPrefix, me) {
    let attrs = fnCall.$arguments.map(
        (ele) => {
            return alias + "." + ele;
        }
    );
    let args = [fnCall.$format].concat(attrs);
    let result = util.format.apply(null, args);
    if (fnCall.hasOwnProperty("$as")) {
        let as = projectionPrefix ? projectionPrefix + "." + fnCall.$as : fnCall.$as;
        result += " as ";
        result += as;
    }
    if (fnCall.hasOwnProperty("$order")) {
        if (fnCall.$order === 0) {
            throw new Error("SORT条件必须是不为0的数值，当前检测到非法值" + fnCall.$order);
        }
        let order = fnCall.$order > 0 ? " ASC" : " DESC";
        result += order;
    }
    //	Added by wangyuef 2017-07-19	implement $fnCall in "where"	Start
    if (fnCall.hasOwnProperty("$where")) {
        if (!me) {
            throw new Error("$fnCall中的where查询，必须传入this指针");
        }
        let where = me.transformWhere(fnCall.$where, result, alias, null);
        result += where;
    }
    //	Added by wangyuef 2017-07-19	implement $fnCall in "where"	End
    return result;
}

function convertExecNodeToSQL(sql, node, parentName, relName, joinInfo, projectionPrefix) {
    let alias = relName;
    if (sql.usedNames[relName]) {
        alias = relName + "_" + sql.usedNames[relName];
        sql.usedNames[relName]++;
    }
    else {
        sql.usedNames[relName] = 1;
    }
    node.alias = alias;

    if (joinInfo) {
        sql.from += " left join `" + relName + "` `" + alias + "` on `" + parentName + "`.`" + joinInfo.localColumnName + "` = `" + alias + "`.`" + joinInfo.refColumnName + "`";
    }
    else {
        sql.from += " `" + relName + "` ";
        if (alias !== relName) {
            sql.from += " `" + alias + "` ";
        }
    }

    for (let projection in node.projection) {
        if (sql.projection.length > 0) {
            sql.projection += ", ";
        }

        if (projection.toLowerCase().startsWith("$fncall")) {
            // 处理函数调用的projection
            sql.projection += convertFnCallToSQLFormat(alias, node.projection[projection], projectionPrefix);
        }
        else {
            sql.projection += "`" + alias + "`.`";
            sql.projection += projection + "`";
            if (projectionPrefix) {
                sql.projection += " as '" + projectionPrefix + projection + "'";
            }
        }
    }

    if (node.query) {
        // 对subquery作特殊处理
        if (node.query.$has) {
            if (sql.subquery && sql.subquery.$has) {
                throw new Error("查询中最多只能有一个$has");
            }
            sql.subquery = assign(sql.subquery, {
                $has: node.query.$has
            });
            delete node.query.$has;
        }
        if (node.query.$hasnot) {
            if (sql.subquery && sql.subquery.$hasnot) {
                throw new Error("查询中最多只能有一个$hasnot");
            }
            sql.subquery = assign(sql.subquery, {
                $hasnot: node.query.$hasnot
            });
            delete node.query.$hasnot;
        }
        let where = this.transformWhere(node.query, undefined, alias, relName);
        if (where.length > 0) {
            if (sql.where.length > 0) {
                sql.where += " AND ";
            }

            sql.where += "(" + where + ")";
        }
    }

    for (let attr in node.sort) {
        if (sql.orderBy.length > 0) {
            sql.orderBy += ",";
        }
        if (attr.toLowerCase().startsWith("$fncall")) {
            // 处理函数调用的projection
            sql.orderBy += convertFnCallToSQLFormat(alias, node.sort[attr], projectionPrefix);
        }
        else {
            if (node.sort[attr] === 1 || node.sort[attr] === -1) {
                // 如果排序的列是as出来的名字，不要用1和-1这两个值，可以避免加上表名
                // 见测试用例tfas3.0
                if (projectionPrefix) {
                    sql.orderBy += "`" + alias + "`.";
                }
                else {
                    sql.orderBy += "`" + relName + "`.";
                }
            }
            sql.orderBy += "`" + attr + "`";

            if (typeof node.sort[attr] != "number" || node.sort[attr] === 0) {
                throw new Error("SORT条件必须是不为0的数值，当前检测到非法值" + node.sort[attr]);
            }
            sql.orderBy += node.sort[attr] > 0 ? " ASC" : " DESC";
        }
    }


    node.joins.forEach(
        (ele, index) => {
            let prefix = (projectionPrefix || "") + ele.attr + ".";
            convertExecNodeToSQL.call(this, sql, ele.node, alias, ele.rel, ele, prefix);
        }
    )
}

class SQLTransformer {
    constructor(schemas, typeConvertor) {
        this.schemas = schemas;
        this.typeConvertor = typeConvertor || convertValueToSQLFormat;
    }

    setSchemas(schemas) {
        this.schemas = schemas;
    }

    transformInsert(tableName, data) {
        let sql = "insert into `";
        sql += tableName;
        sql += "`";

        assert(data instanceof Array);
        let wholeAttrsObject = {};
        data.forEach(
            (ele) => {
                wholeAttrsObject = assign(wholeAttrsObject, ele);
            }
        );
        let attributes = "(", values = "";

        const wholeAttrs = keys(wholeAttrsObject);
        wholeAttrs.forEach(
            (ele, idx) => {
                if (idx !== 0) {
                    attributes += ",";
                }
                attributes += `\`${ele}\``;
            }
        );
        attributes += ")";

        data.forEach(
            (ele, idx) => {
                if (idx !== 0) {
                    values += ",";
                }
                values += "("
                wholeAttrs.forEach(
                    (ele2, idx2) => {
                        if (idx2 !== 0) {
                            values += ",";
                        }
                        const type = this.schemas[tableName].attributes[ele2].type;
                        values += this.typeConvertor(ele[ele2], type);
                    }
                );
                values += ")";
            }
        );

        sql += attributes;
        sql += " values";
        sql += values;
        sql += ";";

        return sql;
    }

    transformUpdate(tableName, data, query) {
        let sql = "update `";
        sql += tableName;
        sql += "`";
        sql += " set ";

        let updatePart = data.$set;
        let incrementPart = data.$inc;

        if (updatePart) {
            keys(updatePart).forEach(
                (ele, idx) => {
                    if (idx > 0) {
                        sql += ",";
                    }

                    sql += "`" + ele + "`";
                    sql += "=";
                    const type = this.schemas[tableName].attributes[ele].type;
                    const value = this.typeConvertor(updatePart[ele], type);
                    sql += value;
                }
            );
        }

        if (incrementPart) {
            keys(incrementPart).forEach(
                (ele, idx) => {
                    if (updatePart || idx > 0) {
                        sql += ",";
                    }

                    sql += "`" + ele + "`";
                    sql += "=";
                    const type = this.schemas[tableName].attributes[ele].type;
                    const value = this.typeConvertor(incrementPart[ele], type);
                    sql += "`" + ele + "`+ (";
                    sql += value;
                    sql += ")";
                }
            );
        }

        sql += " where ";
        sql += this.transformWhere(query, undefined, undefined, tableName);

        return sql;
    }

    transformDelete(name, where) {
        let sql = "";
        sql += "delete from `";
        sql += name;
        sql += "`";
        sql += " where ";
        sql += this.transformWhere(where, undefined, undefined, name);

        return sql;
    }

    transformSubQuery(subquery) {

    }


    transformWhere(where, attr, alias, relName) {
        if (where === null || where === undefined) {
            throw new Error(`属性【${attr}】的值为${where}`);
        }
        let sql = "";
        let type;
        if (relName && this.schemas[relName] && attr && this.schemas[relName].attributes[attr]) {
            type = this.schemas[relName].attributes[attr].type;
        }
        if (typeof where === "object" && Object.getOwnPropertyNames(where).length > 1) {
            if (where.hasOwnProperty("$ref")) {
                /**
                 *    处理一种特殊情况，在$has这样的subquery中，用
                 *        {
			 *			$ref: Object
			 *			$attr: "id"
			 *		}
                 *    这样的格式来传属性
                 */
                assert(Object.getOwnPropertyNames(where).length === 2 && where.hasOwnProperty("$attr"));
                sql += " = ";
                sql += this.typeConvertor(where);
            }
            else {
                if (type === undefined) {
                    for (let field in where) {
                        if (sql.length > 0) {
                            sql += " AND ";
                        }
                        sql += "(" + this.transformWhere({[field]: where[field]}, field, alias, relName) + ")";
                    }
                }
                else {
                    sql += "=";
                    sql += this.typeConvertor(where, type);
                }
            }
        }
        else {
            if (where.hasOwnProperty("$or")) {
                (where.$or).forEach((ele, index) => {
                    if (index > 0) {
                        sql += " OR ";
                    }
                    sql += "(" + this.transformWhere(ele, undefined, alias, relName) + ")";
                });
            }
            else if (where.hasOwnProperty("$and")) {
                (where.$and).forEach((ele, index) => {
                    if (index > 0) {
                        sql += " AND ";
                    }
                    sql += "(" + this.transformWhere(ele, undefined, alias, relName) + ")";
                });
            }
            //  Updated by wangyuef "type" => (..|| typeof ..) 2017-7-19 Start
            else if (where.hasOwnProperty("$eq")) {
                sql += "=";
                sql += this.typeConvertor(where.$eq, type || typeof(where.$eq));
            }
            else if (where.hasOwnProperty("$gt")) {
                sql += ">";
                sql += this.typeConvertor(where.$gt, type || typeof(where.$gt));
            }
            else if (where.hasOwnProperty("$gte")) {
                sql += ">=";
                sql += this.typeConvertor(where.$gte, type || typeof(where.$gte));
            }
            else if (where.hasOwnProperty("$lt")) {
                sql += "<";
                sql += this.typeConvertor(where.$lt, type || typeof(where.$lt));
            }
            else if (where.hasOwnProperty("$lte")) {
                sql += "<=";
                sql += this.typeConvertor(where.$lte, type || typeof(where.$lte));
            }
            else if (where.hasOwnProperty("$ne")) {
                sql += "<>";
                sql += this.typeConvertor(where.$ne, type || typeof(where.$ne));
            }
            else if (where.hasOwnProperty("$in")) {
                sql += " in (";
                if (where.$in.length === 0) {
                    throw new Error(`${attr} in算子的域集合为空`);
                    return;
                }
                where.$in.forEach((ele, index) => {
                    if (index > 0) {
                        sql += ",";
                    }
                    sql += this.typeConvertor(ele, type || typeof(ele));
                });
                sql += ")";
            }
            else if (where.hasOwnProperty("$nin")) {
                sql += " not in (";
                where.$nin.forEach((ele, index) => {
                    if (index > 0) {
                        sql += ",";
                    }
                    sql += this.typeConvertor(ele, type || typeof(ele));
                });
                sql += ")";
            }
            else if (where.hasOwnProperty('$between')) {
                const between = where.$between;
                const left = where.$between.$left;
                const right = where.$between.$right;
                assert(left);
                assert(right);
                if (typeof left === 'object') {
                    sql += left.$closed ? " >= " : " > ";
                    sql += left.$value;
                }
                else {
                    sql += " > ";
                    sql += left;
                }
                sql += " and ";
                sql += "`" + alias + "`.`" + attr + "`";
                if (typeof right === 'object') {
                    sql += right.$closed ? " <= " : " < ";
                    sql += right.$value;
                }
                else {
                    sql += " < ";
                    sql += right;
                }
            }
            else if (where.hasOwnProperty("$exists")) {
                sql += where.$exists ? " is not null" : " is null";
            }
            else {
                // 只支持以上的算子，除此之外如果再有$开关的算子，直接报不支持
                switch (typeof where) {
                    case "object":
                    {
                        if (Object.getOwnPropertyNames(where).length === 1) {
                            for (let attr2 in where) {
                                if (attr2.toLowerCase().startsWith("$fncall")) {
                                    //  Added by wangyuef param "this" 2017-7-19
                                    sql += convertFnCallToSQLFormat(alias, where[attr2], undefined, this);
                                }
                                else if (attr2.startsWith("$")) {
                                    throw new Error("不支持算子" + attr2 + "向SQL的转化");
                                }
                                else {
                                    if (alias) {
                                        sql += "`" + alias + "`";
                                        sql += "."
                                    }
                                    sql += "`" + attr2 + "`";
                                    sql += this.transformWhere(where[attr2], attr2, alias, relName);
                                }
                            }
                        }
                        break;
                    }
                    default:
                    {
                        // 此时应该是“=”算子
                        sql += "=";
                        sql += this.typeConvertor(where, type || typeof where);
                    }
                }
            }
        }
        //  Updated by wangyuef "type" => (..|| typeof ..) 2017-7-19 End

        return sql;
    }


    transformSelect(name, execTree, indexFrom, count, isCounting, usedNames) {
        let sqlObj = {
            projection: "",
            from: "",
            where: "",
            orderBy: "",
            usedNames: usedNames || {}
        };

        convertExecNodeToSQL.call(this, sqlObj, execTree, null, name);

        if (isCounting) {
            sqlObj.projection = "count(1) as `count`";
        }

        let sql = "select ";
        sql += sqlObj.projection;
        sql += " from ";
        sql += sqlObj.from;

        let hasWhere = false;
        if (sqlObj.where.length > 0) {
            hasWhere = true;
            sql += " where ";
            sql += sqlObj.where;
        }
        if (sqlObj.subquery) {
            // 如果有子查询在这里处理
            if (sqlObj.subquery["$has"]) {
                if (hasWhere) {
                    sql += " and ";
                }
                else {
                    sql += " where ";
                }
                hasWhere = true;
                sql += " exists (";
                sql += this.transformSelect(sqlObj.subquery["$has"].name, sqlObj.subquery["$has"].execTree, undefined, undefined, undefined, sqlObj.usedNames);
                sql += ")";
            }

            if (sqlObj.subquery["$hasnot"]) {
                if (hasWhere) {
                    sql += " and ";
                }
                else {
                    sql += " where ";
                }
                hasWhere = true;
                sql += " not exists (";
                sql += this.transformSelect(sqlObj.subquery["$hasnot"].name, sqlObj.subquery["$hasnot"].execTree, undefined, undefined, undefined, sqlObj.usedNames);
                sql += ")";
            }
        }
        if (sqlObj.orderBy) {
            sql += " order by ";
            sql += sqlObj.orderBy;
        }

        if (indexFrom !== undefined) {
            sql += " limit " + indexFrom + ", " + count;
        }

        return sql;
    }
}

module.exports = SQLTransformer;
