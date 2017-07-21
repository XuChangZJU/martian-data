/**
 * Created by Administrator on 2016/6/7.
 */
"use strict";


var expect = require("expect.js");

const SQLTransformer = require("../src/utils/sqlTransformer");

const now = Date.now();
const schemas = {
    user: {
        attributes: {
            name: {
                type: "string"
            },
            age: {
                type: "int"
            },
            time: {
                type: "date"
            },
            id: {
                type: "int"
            },
            gender: {
                type: "string"
            }
        }
    },
    mark: {
        attributes: {
            value: {
                type: 'float',
            },
            user: {
                type: 'ref',
                ref: 'user',
            },
        },
    },
};
const sqlTransformer = new SQLTransformer(schemas);

describe("test sqltransformer", () => {
	it("[st1.0]", (done) => {

		const query = {
			name: 'xc',
			age: 33,
			time: now
		}

		const sql = sqlTransformer.transformWhere(query, undefined, undefined, "user");
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

		const sql = sqlTransformer.transformWhere(query, undefined, undefined, "user");
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

		const sql = sqlTransformer.transformWhere(query, undefined, undefined, "user");
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

		const sql = sqlTransformer.transformWhere(query, undefined, undefined, "user");
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

		const sql = sqlTransformer.transformWhere(query, undefined, undefined, "user");
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

		const sql = sqlTransformer.transformWhere(query, undefined, undefined, "user");
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

		const sql = sqlTransformer.transformWhere(query, undefined, undefined, "user");
		console.log(sql);
		done();
	});

	it("[st1.7]", (done) => {

		const query = {
			id: 1
		};

		const sql = sqlTransformer.transformWhere(query, undefined, undefined, "user");
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

	it("[st3.0]", (done) => {
		const items = [
			{
				name: 'xc',
				age: '35'
			},
			{
				name: 'cg',
				age: '45',
				gender: '男',
			},
			{
				name: 'sld',
				id: 5
			}
		];
		const sql = sqlTransformer.transformInsert("user", items);
		console.log(sql);
		done();
	});

	it("[st4.0]between", (done) => {
		const query = {
			$between:{
				$left: 1000,
				$right: 2000,
			},
		};

		const sql = sqlTransformer.transformWhere(query, 'price', undefined, "user");
		console.log(sql);
		done();
	});

	it("[st4.1]between2", (done) => {
		const query = {
			$between:{
				$left: {
					$closed: true,
					$value: 1000,
				},
				$right: 2000,
			},
		};

		const sql = sqlTransformer.transformWhere(query, 'price', undefined, "user");
		console.log(sql);
		done();
	});

	it("[st5.0]or", (done) => {
		const query = {
			$or: [
				{
					age: {
						$gt: 10,
					},
				},
				{
					age: {
						$lt: 5,
					},
				}
			],
		};

		const sql = sqlTransformer.transformWhere(query, null, null, 'user');
		console.log(sql);
		done();
	});


    it("[st6.0]fnCall", (done) => {
        const query = {
            $fnCall: {
                $format: "ST_AsText(%s)",
	$arguments: ["age"],
                $where: {
                    $lte: 1
                }
            }
        };

        const sql = sqlTransformer.transformWhere(query, null, 'user2', 'user');
        console.log(sql);
        done();
    });

	it("[st7.0]like", (done) => {
		const query = {
			name: {
				$like: 'xc',
			}
		};

		const sql = sqlTransformer.transformWhere(query, null, null, 'user');
		console.log(sql);
		done();
	});
})
