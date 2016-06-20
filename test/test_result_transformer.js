/**
 * Created by Administrator on 2016/6/8.
 */
"use strict";

const resultTransformer = require("../src/utils/resultTransformer");

var expect = require("expect.js");

describe("test result transformer", () => {
    it("[tr0.0]", (done) => {
        const data = {
            "user.id": 1,
            "user.name": "xiaoming"
        };

        const result = resultTransformer.transformSelectResult(data);
        expect(result).to.be.an("object");
        expect(result).to.have.property("user");
        expect(result.user).to.be.an("object");
        expect(result.user).to.have.property("id");
        expect(result.user.id).to.be(1);
        done();
    });


    it("[tr0.1]", (done) => {
        const data = {
            id: 123,
            "user.id": 1,
            "user.name": "xiaoming",
            "user.address.city": "hangzhou",
            "user.address.district": "xihu"
        };

        const result = resultTransformer.transformSelectResult(data);
        expect(result).to.be.an("object");
        expect(result.id).to.be(123);
        expect(result).to.have.property("user");
        expect(result.user).to.be.an("object");
        expect(result.user).to.have.property("id");
        expect(result.user.id).to.be(1);
        expect(result.user.address).to.be.an("object");
        expect(result.user.address.district).to.be("xihu");
        done();
    });

    it("[tr0.1]", (done) => {
        const data = [
            {
                id: 123,
                "user.id": 1,
                "user.name": "xiaoming",
                "user.address.city": "hangzhou",
                "user.address.district": "xihu"
            },
            {
                "user.id": 2,
                "user.name": "xiaohong"

            }];

        const result = resultTransformer.transformSelectResult(data);
        expect(result).to.be.an("array");
        expect(result).to.have.length(2);
        const result1 = result[0];
        const result2 = result[1];
        expect(result1.id).to.be(123);
        expect(result1).to.have.property("user");
        expect(result1.user).to.be.an("object");
        expect(result1.user).to.have.property("id");
        expect(result1.user.id).to.be(1);
        expect(result1.user.address).to.be.an("object");
        expect(result1.user.address.district).to.be("xihu");

        expect(result2).to.be.an("object");
        expect(result2.user).to.be.an("object");
        expect(result2.user.id).to.be(2);
        expect(result2.user.name).to.be("xiaohong");
        done();
    });
});
