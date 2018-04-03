/**
 * Created by Administrator on 2018/2/9.
 */
const keys = require("lodash/keys");
const merge = require("lodash/merge");
const assign = require("lodash/assign");
const assert = require("assert");
const values = require("lodash/values");
const constants = require("../constants");

function compareAttr(comparer, stander) {
    if (keys(stander).some((ele)=>stander[ele] !== comparer[ele])) {
        return false;
    }
    return true;
}

function satisfy(entity, query, relName) {
    let type;
    if (typeof query === "object" && Object.getOwnPropertyNames(query).length > 1) {
        if (query.hasOwnProperty("$ref")) {
            // 暂不支持多表的storage查询;
            return false;
            /**
             *    处理一种特殊情况，在$has这样的subquery中，用
             *        {
			 *			$ref: Object
			 *			$attr: "id"
			 *		  }
             *    这样的格式来传属性
             */
            // assert(Object.getOwnPropertyNames(query).length === 2 && query.hasOwnProperty("$attr"));
            // if (entity[relName]) {
            //
            // }
        }
        else {
            if (type === undefined) {
                if (keys(query).some((filed)=>!satisfy(entity, {[filed]: query[filed]}, filed))) {
                    return false;
                }
            }
            else {
                assert(false);
            }
        }
    }
    else {
        if (query.hasOwnProperty("$or")) {
            if ((query.$or).every((ele, index) => !satisfy(entity, ele))) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$and")) {
            if ((query.$and).some((ele) =>!satisfy(entity, ele))) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$eq")) {
            if (entity[relName] !== query["$eq"]) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$gt")) {
            if (entity[relName] <= query["$gt"]) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$gte")) {
            if (entity[relName] < query["$gte"]) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$lt")) {
            if (entity[relName] >= query["$lt"]) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$lte")) {
            if (entity[relName] > query["$lte"]) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$ne")) {
            if (entity[relName] === query["$ne"]) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$in")) {
            if (!(query["$in"].includes(entity[relName]))) {
                return false;
            }
        }
        else if (query.hasOwnProperty("$nin")) {
            if ((query["$nin"].includes(entity[relName]))) {
                return false;
            }
        }
        else if (query.hasOwnProperty('$between')) {
            const between = query.$between;
            const left = query.$between.$left;
            const right = query.$between.$right;
            assert(left);
            assert(right);
            if (typeof left === 'object') {
                if (left.$closed && entity[relName] < left.$value) {
                    return false;
                }
                else if (entity[relName] <= left.$value) {
                    return false;
                }
            }
            else {
                if (right.$closed && entity[relName] > right.$value) {
                    return false;
                }
                else if (entity[relName] >= right.$value) {
                    return false;
                }
            }
        }
        else if (query.hasOwnProperty("$exists")) {
            if (query["$exists"]) {
                if (!entity[relName]) {
                    return false;
                }
            }
            if (!query["$exists"]) {
                if (entity[relName]) {
                    return false;
                }
            }
        }
        else if (query.hasOwnProperty("$like")) {
            return false;
            // if (!entity[relName].startsWith(query.$like)) {
            //     return false;
            // }
        }
        else {
            switch (typeof query) {
                case "object":
                {
                    let result = true;
                    keys(query).forEach(
                        (ele)=> {
                            if (typeof query[ele] === "object") {
                                if (!satisfy(entity, query[ele], ele)) {
                                    result = false;
                                }
                            }
                            else {
                                if (!compareAttr(entity, query)) {
                                    result = false;
                                }
                            }
                        }
                    );
                    return result;
                }
                default:
                {
                    console.error(query);
                    assert(false);
                }
            }
        }
    }

    return true;
}

class mtStorage {
    constructor(entities) {
        this.entities = entities || {};
    }

    mergeGlobalEntities(obj, cacheExpiredTime) {
        keys(obj).forEach(
            (key) => {
                if (obj[key] instanceof Array) {
                    let objTmp = {};
                    obj[key].forEach(
                        (ele) => objTmp[ele.id] = ele
                    );
                    obj[key] = objTmp;
                }
                if (obj.cover) {
                    this.globalData.entities[key] = obj[key].map((ele)=>assign({}, ele, {$timeout: Date.now() + cacheExpiredTime || constants.storageInterval}));
                }
                else {
                    keys(obj[key]).forEach(
                        (id) => {
                            let exitsEle = this.entities[key] && this.entities[key][id];
                            if (exitsEle) {
                                exitsEle = merge({}, exitsEle, obj[key][id]);
                            }
                            else {
                                this.entities[key] = this.entities[key] || {};
                                this.entities[key][id] = assign({}, obj[key][id], {$timeout: Date.now() + cacheExpiredTime || constants.storageInterval});
                            }
                        }
                    )
                }
            }
        )
    }

    clearStorage(key) {
        if (this.entities && this.entities[key]) {
            this.entities[key] = {};
        }
        if (!key) {
            this.entities = {};
        }
    }

    clearStorageBy(table, query) {
        keys(this.entities[table]).forEach(
            (eleKey) => {
                if (!query) {
                    this.entities = {};
                }
                else if (satisfy(this.entities[table][eleKey], query)) {
                    //  若是发现缓存已过期，则删除
                    delete this.entities[table][eleKey];
                }
            }
        );
    }

    getEntities(table, query) {
        const result = [];
        keys(this.entities[table]).forEach(
            (eleKey) => {
                if (!query) {
                    //  若是发现缓存已过期，则删除
                    if (this.entities[table][eleKey] && this.entities[table][eleKey].$timeout < Date.now()) {
                        delete this.entities[table][eleKey];
                }
                    else {
                        result.push(this.entities[table][eleKey]);
                    }
                }
                else if (satisfy(this.entities[table][eleKey], query)) {
                    //  若是发现缓存已过期，则删除
                    if (this.entities[table][eleKey] && this.entities[table][eleKey].$timeout < Date.now()) {
                        delete this.entities[table][eleKey];
                    }
                    else {
                        result.push(this.entities[table][eleKey]);
                    }
                }
            }
        );
        return result;
    }
}

module.exports = mtStorage;