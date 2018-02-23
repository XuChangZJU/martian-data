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
        .then(
            () => {
                return uda.createSchemas()
                    .then(
                        () => {
                            return uda.insert({
                                    name: "house",
                                    data: {
                                        buildAt: Date.now(),
                                        status: "offline"
                                    }
                                })
                                .then(
                                    (row) => {
                                        let id = row.hasOwnProperty("id") ? row.id : row._id;
                                        return uda.updateOneById({
                                                name: "house",
                                                data: {
                                                    $set: {
                                                        status: "free"
                                                    }
                                                },
                                                id
                                            })
                                            .then(
                                                (row) => {
                                                    let id = row.hasOwnProperty("id") ? row.id : row._id;
                                                    return uda.removeOneById({
                                                        name: "house",
                                                        id
                                                    });
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
                    uda.insert({
                            name: "house",
                            data: items
                        })
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

    it("[cud1.4]increment in mysql", () => {
        let _schema = JSON.parse(JSON.stringify(schema2));

        _schema.house.source = "mysql";
        return uda.setSchemas(_schema)
            .then(
                () => {
                    const item =
                    {
                        buildAt: Date.now(),
                        status: "free"
                    };
                    return uda.insert({
                            name: "house",
                            data: item
                        })
                        .then(
                            (result) => {
                                return uda.updateOneById({
                                        name: 'house',
                                        data: {
                                            $inc: {
                                                buildAt: -10000
                                            }
                                        },
                                        id: result.id
                                    })
                                    .then(
                                        (result2) => {
                                            return uda.findById({
                                                    name: 'house',
                                                    data: {
                                                        buildAt: 1
                                                    },
                                                    id: result.id
                                                })
                                                .then(
                                                    (result3) => {
                                                        expect(result3.buildAt + 10000).to.eql(result.buildAt);
                                                        return Promise.resolve();
                                                    }
                                                );
                                        }
                                    );
                            }
                        );
                }
            );
    });


    it("[cud1.5]increment in mysql", () => {
        let _schema = JSON.parse(JSON.stringify(schema2));

        _schema.house.source = "mysql";
        return uda.setSchemas(_schema)
            .then(
                () => {
                    const item =
                    {
                        buildAt: Date.now(),
                        status: "free"
                    };
                    return uda.insert({
                            name: "house",
                            data: item
                        })
                        .then(
                            (result) => {
                                const promises = [];
                                for (let i = 0; i < 20; i++) {
                                    promises.push(
                                        uda.updateOneById({
                                            name: 'house',
                                            data: {
                                                $inc: {
                                                    buildAt: -10000
                                                }
                                            },
                                            id: result.id
                                        })
                                    );
                                }
                                return Promise.all(promises)
                                    .then(
                                        () => {
                                            return uda.findById({
                                                    name: 'house',
                                                    data: {
                                                        buildAt: 1
                                                    },
                                                    id: result.id
                                                })
                                                .then(
                                                    (result2) => {
                                                        expect(result2.buildAt + 10000 * 20).to.eql(result.buildAt);
                                                        return Promise.resolve();
                                                    }
                                                )
                                        }
                                    )
                            }
                        );
                }
            );
    });

    it("[cud1.6]$set could be omitted", () => {
        let _schema = JSON.parse(JSON.stringify(schema2));

        _schema.house.source = "mysql";
        return uda.setSchemas(_schema)
            .then(
                () => {
                    const item =
                    {
                        buildAt: Date.now(),
                        status: "free"
                    };
                    return uda.insert({
                            name: "house",
                            data: item
                        })
                        .then(
                            (result) => {
                                return uda.updateOneById({
                                        name: 'house',
                                        data: {
                                            buildAt: 100000
                                        },
                                        id: result.id
                                    })
                                    .then(
                                        (result2) => {
                                            console.log(result2);
                                            return Promise.resolve();
                                        }
                                    )
                            }
                        );
                }
            );
    });

    it("[cud1.7]mtStorage在本地数据源也可起作用", () => {
        let _schema = JSON.parse(JSON.stringify(schema));

        _schema.houseInfo.source = "mysql";
        return uda.setSchemas(_schema)
            .then(
                () => {
                    const item =
                    {
                        area: 100.00,
                        floor: 3
                    };
                    return uda.insert({
                            name: "houseInfo",
                            data: item
                        })
                        .then(
                            (result) => {
                                return uda.findById({
                                    name: "houseInfo",
                                    id: result.id,
                                    useStorage: true
                                }).then(
                                    (house)=> {
                                        expect(house.area).to.equal(100);
                                        return uda.updateOneById({
                                                name: 'houseInfo',
                                                data: {
                                                    area: 100000
                                                },
                                                id: result.id
                                            })
                                            .then(
                                                (result2) => {
                                                    return uda.findById({
                                                        name: "houseInfo",
                                                        id: result.id,
                                                        useStorage: true
                                                    }).then(
                                                        (house)=> {
                                                            expect(house.area).to.equal(100);
                                                            return uda.findById({
                                                                name: "houseInfo",
                                                                id: result.id
                                                            }).then(
                                                                (house)=> {
                                                                    expect(house.area).to.equal(100000);
                                                                    return Promise.resolve();
                                                                })
                                                        })
                                                }
                                            )
                                    }
                                )
                            }
                        );
                }
            );
    });

    after((done) => {
        uda.disconnect()
            .then(done);
    });

})
