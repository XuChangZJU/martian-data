"use strict";



var expect = require("expect.js");

const uda = require("../src/UnifiedDataAccess");
const dataSource = require("./def/dataSource");
const schema6 = require("./def/schemas/schema6.js");
const now = Date.now();


describe("test save object in mysql", function() {

    this.timeout(5000);

    before((done) => {
        uda.connect(dataSource)
            .then(
                (result) => {
                    let _schema6 = JSON.parse(JSON.stringify(schema6));
                    uda.setSchemas(_schema6);
                    uda.dropSchemas()
                    	.then(
                    		() => {
                    			uda.createSchemas()
                    				.then(
                    					() => {
                    						done();
                    					},
                    					(err) => {
                    						done(err);
                    					}
                    				);
                    		},
                    		(err) => {
                    			done(err);
                    		}
                    	)

                },
                (err) => {
                    done(err);
                }
            );
    });

    it("[to0.0]", (done) => {
    	let information = {
    		id: 1,
    		name: "xc",
    		age: "33",
    		skills: [
    			{
    				name: "programming",
    				rating: "A"
    			},
    			{
    				name: "playing game",
    				rating: "B"
    			}
    		],
    		birth: new Date(1983, 11, 10)
    	};

    	let _information = JSON.parse(JSON.stringify(information));


		uda.insert("info", {
				information: _information
			})
			.then(
				(result) => {
					uda.findById("info", {
							information: 1
						}, result.id)
						.then(
							(result2)  => {
								expect(result2).to.be.an("object");
								let infoResult = result2.information;
								expect(infoResult.name).to.eql(information.name);
								expect(infoResult.age).to.eql(information.age);
								expect(infoResult.skills).to.be.an("array");
								done();
							},
							(err) => {
								done(err);
							}
						);
				},
				(err) => {
					done(err);
				}
			);
    });


    after((done) => {
        uda.disconnect()
            .then(done);
    });

});

