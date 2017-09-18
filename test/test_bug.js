/**
 * Created by Administrator on 2016/10/10.
 */
"use strict";
const expect = require("expect.js");

const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dataSource = require("./def/dataSource");
const schema = require("./def/schemas/schema8");
const now = new Date();

describe("test bugs", function() {
    this.timeout(5000);
    before(() => {
        return uda.connect(dataSource)
            .then(
                () => {
                    const _schema = JSON.parse(JSON.stringify(schema));
                    return uda.setSchemas(_schema)
                        .then(
                            () => {
                                return uda.dropSchemas()
                                    .then(
                                        () => {
                                            return uda.createSchemas();
                                        }
                                    );
                            }
                        );
                }
            );
    });

    it("[tbugs0.0]测试单引号转义", () => {
        let info = {
            name: "mike's information"
        };

        return uda.insert({
            name: "info",
            data: info
        });
    })

})