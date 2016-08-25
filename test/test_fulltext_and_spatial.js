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


describe("{tfas2}测试地理信息的查询", function() {
	this.timeout(5000);

	before((done) => {
		uda.connect(dataSource)
			.then(
				() => {
					let schema2;
					schema2 = JSON.parse(JSON.stringify(schema));
					return uda.setSchemas(schema2)
						.then(
							() => {
								return uda.dropSchemas()
									.then(
										() => {
											return uda.createSchemas()
												.then(
													() => {
														done();
													}
												);
										}
									);
							}
						)
				}
			)
			.catch(done);
	})

	it("{tfas2.0} 插入和查询地理信息 MYSQL", (done) => {
		let hangzhou = {
			name: "杭州",
			loc: {
				type: "Point",
				coordinates: [120.31, 30.22]
			},
			desc: "浙江省会，美丽的旅游城市，互联网之都"
		};

		uda.insert("city", hangzhou)
			.then(
				() => {
					return uda.find("city", {
							name: 1,
							loc: 1
						}, {
							loc: {
								type: "Point",
								coordinates: [120.31, 30.22]
							}
						}, null, 0, 10)
						.then(
							(list) => {
								expect(list).to.have.length(1);
								expect(list[0].name).to.eql("杭州");
								let shiyanshi = {
									name: "十堰市",
									loc: {type:"MultiPolygon", coordinates:[[[[111.2476,32.948],[111.2476,32.9315],[111.2476,32.9041],[111.2476,32.8821],[111.2585,32.8821],[111.2805,32.8986],[111.2915,32.8931],[111.2915,32.8601],[111.3135,32.8491],[111.3354,32.8326],[111.3684,32.8326],[111.3904,32.8162],[111.4233,32.7502],[111.4453,32.7502],[111.4673,32.7722],[111.4783,32.7557],[111.4563,32.7283],[111.4453,32.7393],[111.4453,32.7283],[111.4233,32.7338],[111.4673,32.7173],[111.4783,32.6953],[111.5222,32.6678],[111.5332,32.6294],[111.5332,32.6129],[111.5552,32.6074],[111.5552,32.5964],[111.5771,32.5909],[111.5881,32.569],[111.5771,32.547],[111.5552,32.547],[111.5552,32.525],[111.5332,32.5305],[111.5112,32.5195],[111.5332,32.4921],[111.5112,32.4921],[111.5002,32.4756],[111.4783,32.4701],[111.4893,32.4426],[111.4783,32.4371],[111.4783,32.4261],[111.4233,32.4207],[111.4233,32.4097],[111.4124,32.4097],[111.3794,32.4207],[111.3684,32.4042],[111.3245,32.3822],[111.3025,32.3492],[111.2805,32.3438],[111.2366,32.3163],[111.2256,32.3053],[111.2366,32.2943],[111.2146,32.2778],[111.2366,32.2668],[111.2366,32.2449],[111.2585,32.2174],[111.2366,32.1844],[111.2366,32.168],[111.2146,32.157],[111.2256,32.1405],[111.1926,32.1295],[111.1816,32.1185],[111.1487,32.1185],[111.0828,32.0856],[111.0828,32.0746],[111.0828,32.0581],[111.0718,32.0416],[111.0828,32.0142],[111.0718,32.0087],[111.0608,32.0032],[111.0278,32.0142],[111.0059,32.0087],[111.0059,31.9977],[110.9949,31.9922],[111.0059,31.9592],[110.9839,31.9427],[110.9949,31.9427],[110.9729,31.9427],[110.9839,31.9208],[110.9839,31.9098],[111.0059,31.8823],[111.0278,31.8494],[111.0168,31.8329],[110.9949,31.8109],[110.9949,31.8219],[110.9949,31.8549],[110.9839,31.8604],[110.9729,31.8439],[110.9509,31.8713],[110.9399,31.8768],[110.918,31.8658],[110.918,31.8604],[110.885,31.8768],[110.874,31.8549],[110.8521,31.8494],[110.7971,31.8604],[110.7861,31.8439],[110.7642,31.8439],[110.7642,31.8329],[110.7642,31.8274],[110.7532,31.8384],[110.7092,31.8439],[110.6982,31.8494],[110.6763,31.8604],[110.6543,31.8439],[110.5884,31.8549],[110.5774,31.8494],[110.5444,31.8494],[110.5225,31.8384],[110.5005,31.8549],[110.4785,31.8439],[110.4675,31.8109],[110.4565,31.8054],[110.4236,31.8219],[110.4016,31.8164],[110.3906,31.8054],[110.3687,31.7999],[110.3687,31.767],[110.3467,31.7615],[110.2698,31.7615],[110.2698,31.745],[110.2368,31.734],[110.2148,31.6736],[110.2368,31.6681],[110.2368,31.6461],[110.2039,31.6351],[110.2039,31.6241],[110.2258,31.6132],[110.2368,31.5967],[110.2368,31.5802],[110.2258,31.5582],[110.2148,31.5747],[110.1929,31.5802],[110.1819,31.6022],[110.127,31.6187],[110.127,31.6022],[110.083,31.5802],[110.061,31.5417],[110.0391,31.5417],[110.0281,31.5198],[110.0061,31.5253],[109.9841,31.5088],[109.9622,31.5088],[109.9512,31.5198],[109.8962,31.5198],[109.8633,31.5253],[109.8633,31.5417],[109.8193,31.5637],[109.8083,31.5637],[109.8083,31.5527],[109.7314,31.5472],[109.7205,31.5527],[109.7314,31.5692],[109.7424,31.5747],[109.7424,31.5967],[109.7644,31.6022],[109.7424,31.6296],[109.7424,31.6626],[109.7314,31.6846],[109.7314,31.6956],[109.7095,31.701],[109.6875,31.7175],[109.6436,31.723],[109.6216,31.712],[109.5886,31.7285],[109.5996,31.745],[109.5886,31.7505],[109.5886,31.778],[109.5996,31.7999],[109.6326,31.7999],[109.6436,31.8109],[109.6106,31.8494],[109.6106,31.8823],[109.5886,31.8933],[109.5996,31.9153],[109.6216,31.9208],[109.6326,31.9373],[109.6216,31.9702],[109.5886,31.9977],[109.5886,32.0306],[109.5996,32.0636],[109.6216,32.0746],[109.6106,32.0911],[109.6216,32.1021],[109.5886,32.1295],[109.5886,32.1735],[109.6106,32.2064],[109.5667,32.2229],[109.5557,32.2174],[109.5557,32.2339],[109.5227,32.2668],[109.5227,32.2833],[109.4897,32.2888],[109.5227,32.3328],[109.5007,32.3877],[109.5337,32.3987],[109.5227,32.4316],[109.5447,32.4481],[109.5557,32.4756],[109.5667,32.4811],[109.5776,32.514],[109.6106,32.5195],[109.6216,32.536],[109.6436,32.536],[109.6436,32.547],[109.6216,32.5635],[109.6216,32.5745],[109.6216,32.5909],[109.6326,32.5964],[109.6985,32.5964],[109.7314,32.6074],[109.7534,32.5854],[109.7754,32.5854],[109.8303,32.569],[109.8413,32.58],[109.8523,32.58],[109.8962,32.5909],[109.9292,32.5854],[109.9512,32.569],[109.9731,32.5745],[109.9841,32.5525],[110.0281,32.5415],[110.05,32.5635],[110.061,32.558],[110.072,32.58],[110.094,32.5854],[110.083,32.6074],[110.094,32.6184],[110.127,32.6129],[110.1489,32.5909],[110.2039,32.6349],[110.1599,32.6788],[110.1709,32.7173],[110.1599,32.7612],[110.1379,32.7667],[110.127,32.7777],[110.1379,32.7887],[110.1379,32.8107],[110.127,32.8107],[110.105,32.8326],[110.05,32.8546],[110.0281,32.8711],[110.0061,32.8711],[109.9951,32.8876],[109.9292,32.8876],[109.9072,32.9041],[109.8633,32.9095],[109.8523,32.8876],[109.7974,32.8766],[109.7644,32.9041],[109.7864,32.9865],[109.7974,33.0688],[109.7754,33.0688],[109.7534,33.0853],[109.7205,33.0963],[109.6875,33.1183],[109.6765,33.1128],[109.6326,33.1128],[109.6106,33.1073],[109.5886,33.1183],[109.5667,33.1183],[109.5667,33.1238],[109.5557,33.1183],[109.5117,33.1403],[109.4348,33.1458],[109.4568,33.1677],[109.4788,33.1842],[109.4897,33.2062],[109.5117,33.2117],[109.5117,33.2172],[109.5007,33.2227],[109.5227,33.2446],[109.5886,33.2281],[109.5996,33.2336],[109.6216,33.2721],[109.6326,33.2721],[109.6545,33.2501],[109.6655,33.2556],[109.6985,33.2556],[109.7205,33.2336],[109.8083,33.2336],[109.8633,33.2501],[109.9292,33.2281],[109.9841,33.2007],[110.0171,33.2062],[110.05,33.1952],[110.061,33.1952],[110.072,33.2062],[110.116,33.2007],[110.1599,33.2117],[110.1709,33.2062],[110.1709,33.1952],[110.2258,33.1622],[110.2917,33.1732],[110.3357,33.1677],[110.3687,33.1842],[110.4236,33.1732],[110.4565,33.1787],[110.4675,33.1732],[110.4785,33.1842],[110.5115,33.2336],[110.5225,33.2281],[110.5334,33.2336],[110.5334,33.2556],[110.5664,33.2501],[110.5884,33.2336],[110.5994,33.1567],[110.6213,33.1458],[110.6433,33.1458],[110.6433,33.1622],[110.6543,33.1622],[110.6873,33.1183],[110.7092,33.0963],[110.7202,33.1018],[110.7202,33.1238],[110.7312,33.1293],[110.7642,33.1512],[110.7751,33.1567],[110.7861,33.1458],[110.8081,33.1622],[110.8191,33.1512],[110.8191,33.1787],[110.8301,33.2007],[110.874,33.2172],[110.918,33.2062],[110.9729,33.2556],[110.9839,33.2556],[111.0278,33.2117],[111.0608,33.1952],[111.0608,33.1732],[111.0498,33.1677],[111.0388,33.1567],[111.0608,33.1567],[111.0718,33.1787],[111.0938,33.1842],[111.0938,33.1732],[111.1047,33.1732],[111.1816,33.1128],[111.1816,33.0908],[111.1926,33.0908],[111.2036,33.0743],[111.1707,33.0579],[111.1487,33.0414],[111.2256,33.0414],[111.2366,33.0359],[111.2695,32.981],[111.2695,32.959],[111.2476,32.948]]],[[[111.1487,32.0416],[111.1267,32.0306],[111.1047,32.0197],[111.0938,31.9977],[111.0938,32.0197],[111.1157,32.0416],[111.1267,32.0361],[111.1377,32.0471],[111.1487,32.0416]]]]},
									desc: "湖北省十堰市的多边形数据"
								};
								return uda.insert("city", shiyanshi)
									.then(
										() => {
											return uda.find("city", {
													name : 1,
													loc: 1,
												}, {
													loc: {type:"MultiPolygon", coordinates:[[[[111.2476,32.948],[111.2476,32.9315],[111.2476,32.9041],[111.2476,32.8821],[111.2585,32.8821],[111.2805,32.8986],[111.2915,32.8931],[111.2915,32.8601],[111.3135,32.8491],[111.3354,32.8326],[111.3684,32.8326],[111.3904,32.8162],[111.4233,32.7502],[111.4453,32.7502],[111.4673,32.7722],[111.4783,32.7557],[111.4563,32.7283],[111.4453,32.7393],[111.4453,32.7283],[111.4233,32.7338],[111.4673,32.7173],[111.4783,32.6953],[111.5222,32.6678],[111.5332,32.6294],[111.5332,32.6129],[111.5552,32.6074],[111.5552,32.5964],[111.5771,32.5909],[111.5881,32.569],[111.5771,32.547],[111.5552,32.547],[111.5552,32.525],[111.5332,32.5305],[111.5112,32.5195],[111.5332,32.4921],[111.5112,32.4921],[111.5002,32.4756],[111.4783,32.4701],[111.4893,32.4426],[111.4783,32.4371],[111.4783,32.4261],[111.4233,32.4207],[111.4233,32.4097],[111.4124,32.4097],[111.3794,32.4207],[111.3684,32.4042],[111.3245,32.3822],[111.3025,32.3492],[111.2805,32.3438],[111.2366,32.3163],[111.2256,32.3053],[111.2366,32.2943],[111.2146,32.2778],[111.2366,32.2668],[111.2366,32.2449],[111.2585,32.2174],[111.2366,32.1844],[111.2366,32.168],[111.2146,32.157],[111.2256,32.1405],[111.1926,32.1295],[111.1816,32.1185],[111.1487,32.1185],[111.0828,32.0856],[111.0828,32.0746],[111.0828,32.0581],[111.0718,32.0416],[111.0828,32.0142],[111.0718,32.0087],[111.0608,32.0032],[111.0278,32.0142],[111.0059,32.0087],[111.0059,31.9977],[110.9949,31.9922],[111.0059,31.9592],[110.9839,31.9427],[110.9949,31.9427],[110.9729,31.9427],[110.9839,31.9208],[110.9839,31.9098],[111.0059,31.8823],[111.0278,31.8494],[111.0168,31.8329],[110.9949,31.8109],[110.9949,31.8219],[110.9949,31.8549],[110.9839,31.8604],[110.9729,31.8439],[110.9509,31.8713],[110.9399,31.8768],[110.918,31.8658],[110.918,31.8604],[110.885,31.8768],[110.874,31.8549],[110.8521,31.8494],[110.7971,31.8604],[110.7861,31.8439],[110.7642,31.8439],[110.7642,31.8329],[110.7642,31.8274],[110.7532,31.8384],[110.7092,31.8439],[110.6982,31.8494],[110.6763,31.8604],[110.6543,31.8439],[110.5884,31.8549],[110.5774,31.8494],[110.5444,31.8494],[110.5225,31.8384],[110.5005,31.8549],[110.4785,31.8439],[110.4675,31.8109],[110.4565,31.8054],[110.4236,31.8219],[110.4016,31.8164],[110.3906,31.8054],[110.3687,31.7999],[110.3687,31.767],[110.3467,31.7615],[110.2698,31.7615],[110.2698,31.745],[110.2368,31.734],[110.2148,31.6736],[110.2368,31.6681],[110.2368,31.6461],[110.2039,31.6351],[110.2039,31.6241],[110.2258,31.6132],[110.2368,31.5967],[110.2368,31.5802],[110.2258,31.5582],[110.2148,31.5747],[110.1929,31.5802],[110.1819,31.6022],[110.127,31.6187],[110.127,31.6022],[110.083,31.5802],[110.061,31.5417],[110.0391,31.5417],[110.0281,31.5198],[110.0061,31.5253],[109.9841,31.5088],[109.9622,31.5088],[109.9512,31.5198],[109.8962,31.5198],[109.8633,31.5253],[109.8633,31.5417],[109.8193,31.5637],[109.8083,31.5637],[109.8083,31.5527],[109.7314,31.5472],[109.7205,31.5527],[109.7314,31.5692],[109.7424,31.5747],[109.7424,31.5967],[109.7644,31.6022],[109.7424,31.6296],[109.7424,31.6626],[109.7314,31.6846],[109.7314,31.6956],[109.7095,31.701],[109.6875,31.7175],[109.6436,31.723],[109.6216,31.712],[109.5886,31.7285],[109.5996,31.745],[109.5886,31.7505],[109.5886,31.778],[109.5996,31.7999],[109.6326,31.7999],[109.6436,31.8109],[109.6106,31.8494],[109.6106,31.8823],[109.5886,31.8933],[109.5996,31.9153],[109.6216,31.9208],[109.6326,31.9373],[109.6216,31.9702],[109.5886,31.9977],[109.5886,32.0306],[109.5996,32.0636],[109.6216,32.0746],[109.6106,32.0911],[109.6216,32.1021],[109.5886,32.1295],[109.5886,32.1735],[109.6106,32.2064],[109.5667,32.2229],[109.5557,32.2174],[109.5557,32.2339],[109.5227,32.2668],[109.5227,32.2833],[109.4897,32.2888],[109.5227,32.3328],[109.5007,32.3877],[109.5337,32.3987],[109.5227,32.4316],[109.5447,32.4481],[109.5557,32.4756],[109.5667,32.4811],[109.5776,32.514],[109.6106,32.5195],[109.6216,32.536],[109.6436,32.536],[109.6436,32.547],[109.6216,32.5635],[109.6216,32.5745],[109.6216,32.5909],[109.6326,32.5964],[109.6985,32.5964],[109.7314,32.6074],[109.7534,32.5854],[109.7754,32.5854],[109.8303,32.569],[109.8413,32.58],[109.8523,32.58],[109.8962,32.5909],[109.9292,32.5854],[109.9512,32.569],[109.9731,32.5745],[109.9841,32.5525],[110.0281,32.5415],[110.05,32.5635],[110.061,32.558],[110.072,32.58],[110.094,32.5854],[110.083,32.6074],[110.094,32.6184],[110.127,32.6129],[110.1489,32.5909],[110.2039,32.6349],[110.1599,32.6788],[110.1709,32.7173],[110.1599,32.7612],[110.1379,32.7667],[110.127,32.7777],[110.1379,32.7887],[110.1379,32.8107],[110.127,32.8107],[110.105,32.8326],[110.05,32.8546],[110.0281,32.8711],[110.0061,32.8711],[109.9951,32.8876],[109.9292,32.8876],[109.9072,32.9041],[109.8633,32.9095],[109.8523,32.8876],[109.7974,32.8766],[109.7644,32.9041],[109.7864,32.9865],[109.7974,33.0688],[109.7754,33.0688],[109.7534,33.0853],[109.7205,33.0963],[109.6875,33.1183],[109.6765,33.1128],[109.6326,33.1128],[109.6106,33.1073],[109.5886,33.1183],[109.5667,33.1183],[109.5667,33.1238],[109.5557,33.1183],[109.5117,33.1403],[109.4348,33.1458],[109.4568,33.1677],[109.4788,33.1842],[109.4897,33.2062],[109.5117,33.2117],[109.5117,33.2172],[109.5007,33.2227],[109.5227,33.2446],[109.5886,33.2281],[109.5996,33.2336],[109.6216,33.2721],[109.6326,33.2721],[109.6545,33.2501],[109.6655,33.2556],[109.6985,33.2556],[109.7205,33.2336],[109.8083,33.2336],[109.8633,33.2501],[109.9292,33.2281],[109.9841,33.2007],[110.0171,33.2062],[110.05,33.1952],[110.061,33.1952],[110.072,33.2062],[110.116,33.2007],[110.1599,33.2117],[110.1709,33.2062],[110.1709,33.1952],[110.2258,33.1622],[110.2917,33.1732],[110.3357,33.1677],[110.3687,33.1842],[110.4236,33.1732],[110.4565,33.1787],[110.4675,33.1732],[110.4785,33.1842],[110.5115,33.2336],[110.5225,33.2281],[110.5334,33.2336],[110.5334,33.2556],[110.5664,33.2501],[110.5884,33.2336],[110.5994,33.1567],[110.6213,33.1458],[110.6433,33.1458],[110.6433,33.1622],[110.6543,33.1622],[110.6873,33.1183],[110.7092,33.0963],[110.7202,33.1018],[110.7202,33.1238],[110.7312,33.1293],[110.7642,33.1512],[110.7751,33.1567],[110.7861,33.1458],[110.8081,33.1622],[110.8191,33.1512],[110.8191,33.1787],[110.8301,33.2007],[110.874,33.2172],[110.918,33.2062],[110.9729,33.2556],[110.9839,33.2556],[111.0278,33.2117],[111.0608,33.1952],[111.0608,33.1732],[111.0498,33.1677],[111.0388,33.1567],[111.0608,33.1567],[111.0718,33.1787],[111.0938,33.1842],[111.0938,33.1732],[111.1047,33.1732],[111.1816,33.1128],[111.1816,33.0908],[111.1926,33.0908],[111.2036,33.0743],[111.1707,33.0579],[111.1487,33.0414],[111.2256,33.0414],[111.2366,33.0359],[111.2695,32.981],[111.2695,32.959],[111.2476,32.948]]],[[[111.1487,32.0416],[111.1267,32.0306],[111.1047,32.0197],[111.0938,31.9977],[111.0938,32.0197],[111.1157,32.0416],[111.1267,32.0361],[111.1377,32.0471],[111.1487,32.0416]]]]}
												}, null ,0, 100)
												.then(
													(list2) => {
														expect(list2).to.have.length(1);
														expect(list2[0].name).to.eql("十堰市");
														done();
													}
												)
										}
									)
							}
						);
				}
			)
			.catch(
				done
			);
	});
});

describe("{tfas3}测试地理信息的查询", function() {
	this.timeout(5000);

	before((done) => {
		uda.connect(dataSource)
			.then(
				() => {
					let schema2;
					schema2 = JSON.parse(JSON.stringify(schema));
					return uda.setSchemas(schema2)
						.then(
							() => {
								return uda.dropSchemas()
									.then(
										() => {
											return uda.createSchemas()
												.then(
													() => {
														let hangzhou = {
															name: "杭州",
															loc: {
																type: "Point",
																coordinates: [120.19, 30.26]
															},
															desc: "浙江省会，美丽的旅游城市，互联网之都"
														};
														let beijing = {
															name: "北京",
															loc: {
																type: "Point",
																coordinates: [116.46,39.92]
															},
															desc: "中国首都"
														};
														return uda.insert("city", hangzhou)
															.then(
																() => {
																	return uda.insert("city", beijing)
																		.then(
																			() => {
																				done();
																			}
																		)
																}
															);
													}
												);
										}
									);
							}
						)
				}
			)
			.catch(done);
	})

	it("{tfas3.0} 查询地理信息 MYSQL", (done) => {
		uda.find("city", {
				name: 1,
				$fnCall: {
					$format: "ST_AsText(%s)",
					$arguments: ["loc"],
					$as: "location"
				}
			}, undefined, undefined, 0, 100)
			.then(
				(list) => {
					expect(list).to.have.length(2);

					return uda.find("city", {
							name: 1,
							$fnCall: {
								$format: "ST_AsText(%s)",
								$arguments: ["loc"],
								$as: "location"
							},
							$fnCall2: {
								$format: "ST_Distance_Sphere(%s, Point(120, 30))",
								$arguments: ["loc"],
								$as: "distance"
							}
						}, undefined, {
							distance: 1
						}, 0, 100)
						.then(
							(list2) => {
								expect(list2).to.have.length(2);
								expect(list2[0].name).to.eql("杭州");
								expect(list2[1].name).to.eql("北京");
								expect(list2[0].distance).to.be.lessThan(list2[1].distance);
								console.log(list2);
								done();
							}
						)
				}
			)
			.catch(
				done
			);
	});

	it("{tfas3.1} 查询地理信息 MYSQL 2", (done) => {
		uda.find("city", {
				name: 1,
				$fnCall: {
					$format: "ST_AsText(%s)",
					$arguments: ["loc"],
					$as: "location"
				},
				$fnCall2: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30))",
					$arguments: ["loc"],
					$as: "distance"
				}
			}, undefined, {
				$fnCall2: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30))",
					$arguments: ["loc"],
					$order: 1
				}
			}, 0, 100)
			.then(
				(list2) => {
					expect(list2).to.have.length(2);
					expect(list2[0].name).to.eql("杭州");
					expect(list2[1].name).to.eql("北京");
					expect(list2[0].distance).to.be.lessThan(list2[1].distance);
					console.log(list2);
					done();
				}
			)
			.catch(
				done
			);
	});

	it("{tfas3.2} 查询地理信息 MYSQL 3", (done) => {
		uda.find("city", {
				name: 1,
				$fnCall: {
					$format: "ST_AsText(%s)",
					$arguments: ["loc"],
					$as: "location"
				},
				$fnCall2: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30))",
					$arguments: ["loc"],
					$as: "distance"
				}
			},
			{
				$fnCall: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30)) < 50000",
					$arguments: ["loc"],
				}
			},
			{
				$fnCall2: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30))",
					$arguments: ["loc"],
					$order: 1
				}
			}, 0, 100)
			.then(
				(list2) => {
					expect(list2).to.have.length(1);
					expect(list2[0].name).to.eql("杭州");
					console.log(list2);
					done();
				}
			)
			.catch(
				done
			);
	});


	it("{tfas3.3} 查询地理信息 MYSQL 4", (done) => {
		uda.find("city", {
				name: 1,
				$fnCall: {
					$format: "ST_AsText(%s)",
					$arguments: ["loc"],
					$as: "location"
				},
				$fnCall2: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30))",
					$arguments: ["loc"],
					$as: "distance"
				}
			},
			{
				$fnCall: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30)) < 50000",
					$arguments: ["loc"],
				},
				name: "北京"
			},
			{
				$fnCall2: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30))",
					$arguments: ["loc"],
					$order: 1
				}
			}, 0, 100)
			.then(
				(list2) => {
					expect(list2).to.have.length(0);
					done();
				}
			)
			.catch(
				done
			);
	});


	it("{tfas3.4} 查询地理信息 MYSQL 5", (done) => {
		uda.find("city", {
				name: 1,
				$fnCall: {
					$format: "ST_AsText(%s)",
					$arguments: ["loc"],
					$as: "location"
				},
				$fnCall2: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30))",
					$arguments: ["loc"],
					$as: "distance"
				}
			},
			{
				$fnCall: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30)) < 50000",
					$arguments: ["loc"],
				},
				name: "杭州"
			},
			{
				$fnCall2: {
					$format: "ST_Distance_Sphere(%s, Point(120, 30))",
					$arguments: ["loc"],
					$order: 1
				}
			}, 0, 100)
			.then(
				(list2) => {
					expect(list2).to.have.length(1);
					expect(list2[0].name).to.eql("杭州");
					console.log(list2);
					done();
				}
			)
			.catch(
				done
			);
	});
});

describe("{tfas4}测试全文检索", function() {
	this.timeout(5000);

	before((done) => {
		uda.connect(dataSource)
			.then(
				() => {
					let schema2;
					schema2 = JSON.parse(JSON.stringify(schema));
					return uda.setSchemas(schema2)
						.then(
							() => {
								return uda.dropSchemas()
									.then(
										() => {
											return uda.createSchemas()
												.then(
													() => {
														let cities = [
															{
																name: "杭州",
																loc: {
																	type: "Point",
																	coordinates: [120.19, 30.26]
																},
																desc: "浙江省会，美丽的旅游城市，互联网之都"
															},
															{
																name: "北京",
																loc: {
																	type: "Point",
																	coordinates: [116.46,39.92]
																},
																desc: "中国首都"
															},
															{
																name: "上海",
																loc: {
																	type: "Point",
																	coordinates: [121.48, 31.22]
																},
																desc: "中国最大城市，金融之都"
															},
															{
																name: "成都",
																loc: {
																	type: "Point",
																	coordinates: [104.06, 30.67]
																},
																desc: "中国西部的旅游城市，西部城市中心，美食丰富"
															}
														];

														return Promise.all(
															cities.map(
																(ele) => {
																	return uda.insert("city", ele);
																}
															)
															)
															.then(
																() => {
																	done();
																}
															)
															.catch(
																done
															);
													}
												);
										}
									);
							}
						)
				}
			)
			.catch(done);
	});

	it("{tfas4.0} 全文检索 MYSQL", (done) => {
		uda.find("city", {
				name: 1,
				$fnCall: {
					$format: "ST_AsText(%s)",
					$arguments: ["loc"],
					$as: "location"
				}
			},
			{
				$fnCall: {
					$format: "MATCH(%s, %s) AGAINST ('中国')",
					$arguments: ["name", "desc"]
				}
			},
			undefined, 0, 100)
			.then(
				(list) => {
					expect(list).to.have.length(3);

					return uda.find("city", {
							name: 1,
							$fnCall: {
								$format: "ST_AsText(%s)",
								$arguments: ["loc"],
								$as: "location"
							}
						},
						{
							$fnCall: {
								$format: "MATCH(%s, %s) AGAINST ('旅游')",
								$arguments: ["name", "desc"]
							}
						},
						undefined, 0, 100)
						.then(
							(list2) => {
								expect(list2).to.have.length(2);
								expect(list2[0].name).to.eql("杭州");
								expect(list2[1].name).to.eql("成都");
								console.log(list2);
								done();
							}
						)
				}
			)
			.catch(
				done
			);
	});
});
