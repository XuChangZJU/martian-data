/**
 * Created by Administrator on 2016/6/6.
 */
"use strict";
const assert = require("assert");


function convertValueToSQLFormat(value) {
    switch(typeof value) {
        case "number":
        case "boolean":{
            return new String(value);
        }
        case "string": {
            return "\"".concat(value).concat("\"");
        }
        case "object": {
            if(value instanceof Date) {
                return new String(value.valueOf())
            }
            else {
                return JSON.stringify(value);
            }
        }
        default:
            throw new Error("无法转换的数据值" + value);
    }
}


function convertExecNodeToSQL(sql, node, parentName, relName, joinInfo, projectionPrefix) {
    let alias = relName;
    if(sql.usedNames[relName]) {
        alias = relName + "_" + sql.usedNames[relName];
        sql.usedNames[relName] ++;
    }
    else {
        sql.usedNames[relName] = 1;
    }

    if(joinInfo) {
        sql.from += " left join " + relName + " " + alias + " on " + parentName + "." + joinInfo.localColumnName + " = " + alias + "." + joinInfo.refColumnName;
    }

    for(let projection in node.projection) {
        if(sql.projection.length > 0) {
            sql.projection += ", ";
        }

        sql.projection += alias + ".";
        sql.projection += projection;
        if(projectionPrefix) {
            sql.projection += " as '" + projectionPrefix + projection + "'";
        }
    }

    if(node.query) {
        let where = this.transformWhere(node.query, undefined, alias);
        if(where.length > 0) {
            if(sql.where.length > 0) {
                sql.where += " AND ";
            }

            sql.where += "(" + where + ")";
        }
    }

    for(let attr in node.sort) {
        if(sql.orderBy.length > 0) {
            sql.orderBy += ",";
        }
        sql.orderBy += alias + ".";
        sql.orderBy += attr;

        if(typeof node.sort[attr] != "number" || node.sort[attr] === 0) {
            throw new Error("SORT条件必须是不为0的数值，当前检测到非法值"+ node.sort[attr]);
        }
        sql.orderBy += node.sort[attr] > 0 ? " ASC": " DESC";
    }


    node.joins.forEach(
        (ele, index) => {
            let prefix = (projectionPrefix || "")  + ele.attr + ".";
            convertExecNodeToSQL.call(this, sql, ele.node, alias, ele.rel, ele, prefix);
        }
    )
}

class SQLTransformer {
    transformInsert(tableName, data) {
        let sql = "insert into ";
        sql += tableName;
        let attributes = "(", values = "(";

        for(let attr in data) {
            if(!attributes.endsWith("(")) {
                attributes += ",";
            }
            attributes += attr;
            if(!values.endsWith("(")) {
                values += ",";
            }
            const value = data[attr];
            values += convertValueToSQLFormat(value);
        }
        attributes += ")";
        values += ")";

        sql += attributes;
        sql += " values";
        sql += values;

        return sql;
    }

    transformUpdate(tableName, data, query) {
        let sql = "update ";
        sql += tableName;
        sql += " set";

        switch(Object.getOwnPropertyNames(data).length) {
            case 0: {
                throw new Error("对关系数据库的更新必须设置$set属性");
                return;
            }
            case 1: {
                if(!data.$set) {
                    throw new Error("对关系数据库的更新必须设置$set属性");
                }
                break;
            }
            default: {
                throw new Error("对关系数据库的更新只可设置$set属性");
                return;
            }
        }
        for(let attr in data.$set) {
            if(!sql.endsWith("set")) {
                sql += ",";
            }
            else {
                sql += " ";
            }
            sql += attr;
            sql += "=";
            const value = convertValueToSQLFormat(data.$set[attr]);
            sql += value;
        }

        sql += " where ";
        sql += this.transformWhere(query);

        return sql;
    }

    transformDelete(name, where) {
        let sql = "";
        sql += "delete from ";
        sql += name;
        sql += " where ";
        sql += this.transformWhere(where);

        return sql;
    }


    transformWhere(where, subject, relName) {
        let sql = "";
        if(typeof where === "object" && Object.getOwnPropertyNames(where).length > 1) {
            for(let field in where) {
                if(sql.length > 0) {
                    sql += " AND ";
                }
                sql += "(" + this.transformWhere({[field]: where[field]}, subject, relName) + ")";
            }
        }
        else {
            if(where.hasOwnProperty("$or")) {
                (where.$or).forEach((ele, index) => {
                    if(index > 0) {
                        sql += " OR ";
                    }
                    sql += "(" + this.transformWhere(ele, subject, relName) + ")";
                });
            }
            else if(where.hasOwnProperty("$and")) {
                (where.$and).forEach((ele, index) => {
                    if(index > 0) {
                        sql += " AND ";
                    }
                    sql += "(" + this.transformWhere(ele, subject, relName) + ")";
                });
            }
            else if(where.hasOwnProperty("$eq")) {
                sql += "=";
                sql += convertValueToSQLFormat(where.$eq);
            }
            else if(where.hasOwnProperty("$gt")) {
                sql += ">";
                sql += convertValueToSQLFormat(where.$gt);
            }
            else if(where.hasOwnProperty("$gte")) {
                sql += ">=";
                sql += convertValueToSQLFormat(where.$gte);
            }
            else if(where.hasOwnProperty("$lt")) {
                sql += "<";
                sql += convertValueToSQLFormat(where.$lt);
            }
            else if(where.hasOwnProperty("$lte")) {
                sql += "<=";
                sql += convertValueToSQLFormat(where.$lte);
            }
            else if(where.hasOwnProperty("$ne")) {
                sql += "<>";
                sql += convertValueToSQLFormat(where.$ne);
            }
            else if(where.hasOwnProperty("$in")) {
                sql += " in (";
                where.$in.forEach((ele, index) => {
                    if(index > 0) {
                        sql += ",";
                    }
                    sql += convertValueToSQLFormat(ele);
                });
                sql += ")";
            }
            else if(where.hasOwnProperty("$nin")) {
                sql += " not in (";
                where.$in.forEach((ele, index) => {
                    if(index > 0) {
                        sql += ",";
                    }
                    sql += convertValueToSQLFormat(ele);
                });
                sql += ")";
            }
            else if(where.hasOwnProperty("$exists")) {
                sql += where.$exists ? " not null" : " is null";
            }
            else {
                // 只支持以上的算子，除此之外如果再有$开关的算子，直接报不支持
                switch(typeof where) {
                    case "object": {
                        if(Object.getOwnPropertyNames(where).length === 1) {
                            for(let attr in where) {
                                if(attr.startsWith("$")) {
                                    throw new Error("不支持算子"+ attr + "向SQL的转化");
                                }
                                else {
                                    if(relName) {
                                        sql += relName;
                                        sql += "."
                                    }
                                    sql += attr;
                                    sql += this.transformWhere(where[attr]);
                                }
                            }
                        }
                        break;
                    }
                    default: {
                        // 此时应该是“=”算子
                        sql += "=";
                        sql += convertValueToSQLFormat(where);
                    }
                }
            }
        }

        return sql;
    }


    transformSelect(name, execTree, indexFrom, count) {
        let sqlObj = {
            projection : "",
            from : new String(name),
            where : "",
            orderBy : "",
            usedNames: {}
        };

        convertExecNodeToSQL.call(this, sqlObj, execTree, null, name);

        let sql = "select ";
        sql += sqlObj.projection;
        sql += " from ";
        sql += sqlObj.from;
        if(sqlObj.where.length > 0) {
            sql += " where ";
            sql += sqlObj.where;
        }
        if(sqlObj.orderBy) {
            sql += " order by ";
            sql += sqlObj.orderBy;
        }

        if(indexFrom !== undefined) {
            sql += " limit " + indexFrom + ", " + count;
        }

        return sql;
    }
}


let sqlTransformer = new SQLTransformer();

module.exports = sqlTransformer;
