/**
 * Created by Administrator on 2016/6/7.
 */
"use strict";


var expect = require("expect.js");

const sqlTransformer = require("../src/utils/sqlTransformer");

const now = Date.now();

describe("test sqltransformer", () => {
    it("[st1.0]", (done) => {

        const query = {
            name: 'xc',
            age: 33,
            time: now
        }

        const sql = sqlTransformer.transformWhere(query);
        console.log(sql);
        done();
    });

    it("[st1.1]", (done) => {

        const query = {
            $and :[
                {name: 'xc'},
                {age: 33},
                {time: now}
            ]
        }

        const sql = sqlTransformer.transformWhere(query);
        console.log(sql);
        done();
    });

    it("[st1.2]", (done) => {

        const query = {
            $or :[
                {name: 'xc'},
                {age: 33},
                {time: now}
            ]
        }

        const sql = sqlTransformer.transformWhere(query);
        console.log(sql);
        done();
    });

    it("[st1.3]", (done) => {

        const query = {
            $or :[
                {name: 'xc'},
                {age: 33},
                {time: now}
            ]
        }

        const sql = sqlTransformer.transformWhere(query);
        console.log(sql);
        done();
    });


    it("[st1.4]", (done) => {

        const query ={
            name: {
                $eq: 'xc'
            },
            age: {
                $lt: 33
            },
            time: {
                $ne: now
            }
        }

        const sql = sqlTransformer.transformWhere(query);
        console.log(sql);
        done();
    });

    it("[st1.5]", (done) => {

        const query ={
            name: {
                $in: ['xc', 'cg', 'sld']
            },
            age: {
                $lt: 33
            },
            time: {
                $ne: now
            }
        }

        const sql = sqlTransformer.transformWhere(query);
        console.log(sql);
        done();
    });

    it("[st1.6]", (done) => {

        const query ={
            $or: [
                {name: {
                    $in: ['xc', 'cg', 'sld']
                }},
                {age: {
                    $lt: 33
                }},
                {time: {
                    $ne: now
                }}
            ]
        }

        const sql = sqlTransformer.transformWhere(query);
        console.log(sql);
        done();
    });

    it("[st1.7]", (done) => {

        const query = {
            id: 1
        };

        const sql = sqlTransformer.transformWhere(query);
        console.log(sql);
        done();
    });

    it("[st2.0]", (done) => {
        const updatePart = {
            $set: {
                gender: '男'
            }
        };

        const query ={
            $or: [
                {name: {
                    $in: ['xc', 'cg', 'sld']
                }},
                {age: {
                    $lt: 33
                }},
                {time: {
                    $ne: now
                }}
            ]
        }

        const sql = sqlTransformer.transformUpdate("user", updatePart, query);
        console.log(sql);
        done();
    });

})
