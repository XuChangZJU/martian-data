/**
 * Created by Administrator on 2016/8/23.
 */
"use strict";



const expect = require("expect.js");

const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dataSource = require("./def/dataSource");

const schema = {
	city: {
		source: 'mysql',
		attributes: {
			name: {
				type: {
					type: 'string',
					size: 256
				},
				required: true
			},
			loc: {
				type: 'geo',
				required: true
			},
			desc: {
				type: 'text'
			}
		},
		indexes: {
			idx_si_loc: {
				options: {
					spatial: true
				},
				columns: {
					loc: '2dsphere'
				}
			},
			idx_ft_name_desc: {
				options: {
					fulltext: true,
					ngram: true
				},
				columns: {
					name: 'text',
					desc: 'text'
				}
			}
		}
	}
};

describe("{tfas}测试全文索引和地理信息索引的创建", function() {
	this.timeout(5000);

	before((done) => {
		uda.connect(dataSource)
			.then(
				() => {
					done();
				}
			)
			.catch(done);
	})

	it("{tfas1.0} 创建mysql表", (done) => {
		let schema2;
		schema2 = JSON.parse(JSON.stringify(schema));

		uda.setSchemas(schema2)
			.then(
				() => {
					return uda.dropSchemas()
						.then(
							() => {
								return uda.createSchemas()
									.then(
										() => {
											console.log("请去mysql的testorm库中检查city表上是否存在相应的spatial index和fulltext index");
											done();
										}
									);
							}
						);
				}
			)
			.catch(done);

	});

	it("{tfas1.1} 创建mongodb表", (done) => {
		let schema2;
		schema2 = JSON.parse(JSON.stringify(schema));
		schema2.city.source = 'mongodb';

		uda.setSchemas(schema2)
			.then(
				() => {
					return uda.dropSchemas()
						.then(
							() => {
								return uda.createSchemas()
									.then(
										() => {
											console.log("请去mongodb的testorm库中检查city表上是否存在相应的spatial index和fulltext index");
											done();
										}
									);
							}
						);
				}
			)
			.catch(done);

	});
});

