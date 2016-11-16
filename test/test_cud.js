/**
 * Created by Administrator on 2016/6/6.
 */
"use strict";




var expect = require("expect.js");


const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();

const dataSource = require("./def/dataSource");
const schema = require("./def/schemas/schema");
const schema2 = require("./def/schemas/schema2");
const schema3 = require("./def/schemas/schema3");


function insertUpdateDeleteHouse(uda) {
    return uda.dropSchemas()
        .then (
            () => {
                return uda.createSchemas()
                    .then(
                        () => {
                            return uda.insert("house", {
                                    buildAt: Date.now(),
                                    status: "offline"
                                })
                                .then(
                                    (row) => {
                                        let id = row.hasOwnProperty("id") ? row.id: row._id;
                                        return uda.updateOneById("house", {
                                               $set:{
                                                   status: "free"
                                               }
                                            }, id)
                                            .then(
                                                (row) => {
                                                    let id = row.hasOwnProperty("id") ? row.id: row._id;
                                                    return uda.removeOneById("house", id);
                                                }
                                            )
                                    }
                                )
                        }
                    )
            }
        )
}

describe("test_insert_update_delete", ()=> {
    before((done) => {
        uda.connect(dataSource)
            .then(
                (result) => {
                    done();
                },
                (err) => {
                    done(err);
                }
            );
    });

    it("[cud1.0]cud in mongodb", (done) => {
        let _schema = JSON.parse(JSON.stringify(schema2));

        _schema.house.source = "mongodb";
        uda.setSchemas(_schema)
            .then(
                () => {
                    insertUpdateDeleteHouse(uda)
                        .then(
                            () => {
                                done();
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

    it("[cud1.1]cud in mysql", (done) => {
        let _schema = JSON.parse(JSON.stringify(schema2));

        _schema.house.source = "mysql";
        uda.setSchemas(_schema)
            .then(
                () => {
                    insertUpdateDeleteHouse(uda)
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
            );
    });

    it("[cud1.2]sql in mysql", (done) => {
        let sql = "select 1 as 'a.id', 2 as 'b.id'";
        let mysql = uda.getSource("mysql");
        mysql.execSql(sql, true)
            .then(
                (result) => {
                    console.log(result);
                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });

    it("[cud1.3]batch insert in mysql", (done) => {
        let _schema = JSON.parse(JSON.stringify(schema2));

        _schema.house.source = "mysql";
        uda.setSchemas(_schema)
            .then(
                () => {
                    const items = [
                        {
                            buildAt: Date.now(),
                            status: "free"
                        },
                        {
                            buildAt: new Date("1983-11-10"),
                            status: "offline"
                        }
                    ];
                    uda.insert("house", items)
                        .then(
                            (results) => {
                                expect(results).to.be.an("array");
                                expect(results).to.have.length(2);
                                done()
                            },
                            done
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

})
