/**
 * Created by Administrator on 2016/6/15.
 */
"use strict";



var expect = require("expect.js");


const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dataSource = require("./def/dataSource");
const schema = require("./def/schemas/schema");
const now = new Date();

const houseInfos = [
    {
        area: 145.4,
        floor: 1
    },
    {
        area: 67.0,
        floor: 6
    }
];

const houses = [
    {
        buildAt: now,
        status: "verifying"
    },
    {
        buildAt: now,
        status: "offline"
    }
];

const users = [
    {
        name: "xiaoming",
        age: null
    },
    {
        name: "xiaohong",
        age: null
    },
    {
        name: "xiaohai",
        age: 34
    },
    {
        name: "xiaozhu",
        age: 29
    }
];

function initData(uda) {
    return uda.dropSchemas()
        .then(
            () => {
                return uda.createSchemas()
                    .then(
                        () => {
                            let promises = [];
                            users.forEach(
                                (ele, idx) => {
                                    const idx2 = idx;
                                    promises.push(
                                        uda.insert("user", ele)
                                            .then(
                                                (result) => {
                                                    users[idx2] = result;
                                                    return Promise.resolve();
                                                },
                                                (err) => {
                                                    return Promise.reject(err);
                                                }
                                            ));
                                }
                            )
                            return Promise.all(promises)
                                .then(
                                    ()=> {
                                        let promises2 = [];
                                        houseInfos.forEach(
                                            (ele, index) => {
                                                promises2.push(
                                                    uda.insert("houseInfo", ele)
                                                        .then(
                                                            (result) => {
                                                                houses[index].houseInfo = result;
                                                                return uda.insert("house", houses[index])
                                                                    .then(
                                                                        (hItem) => {
                                                                            houses[index] = hItem;
                                                                            let contract = {
                                                                                owner: users[index*2],
                                                                                renter: users[index *2 + 1],
                                                                                price: 2000
                                                                            };
                                                                            return uda.insert("contract", contract)
                                                                                .then(
                                                                                    (result) => {
                                                                                        return uda.updateOneById("house", {
                                                                                                $set: {
                                                                                                    contract: result
                                                                                                }
                                                                                            }, (hItem.id || hItem._id))
                                                                                            .then(
                                                                                                () => {
                                                                                                    return Promise.resolve();
                                                                                                },
                                                                                                (err) => {
                                                                                                    return Promise.reject(err);
                                                                                                }
                                                                                            )
                                                                                    },
                                                                                    (err) => {
                                                                                        return Promise.reject(err);
                                                                                    }
                                                                                );
                                                                        },
                                                                        (err) => {
                                                                            return Promise.reject();
                                                                        }
                                                                    );
                                                            }
                                                        )
                                                );
                                            }
                                        );
                                        return Promise.all(promises2)
                                    },
                                    (err) => {
                                        return Promise.reject(err);
                                    }
                                )
                        },
                        (err) => {
                            return Promise.reject(err);
                        }
                    )
            },
            (err) => {
                return Promise.reject(err);
            }
        )
}


describe("test transaction", function() {
    this.timeout(5000);
    before((done) => {
        uda.connect(dataSource)
            .then(
                () => {
                    const _schema = JSON.parse(JSON.stringify(schema));
                    uda.setSchemas(_schema)
                        .then(
                            () => {
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
                                    );
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

    it("[tt0.0] rollback transaction in mysql", (done) => {
        uda.startTransaction("mysql")
            .then(
                () => {
                    const houseInfo = JSON.parse(JSON.stringify(houseInfos[0]));
                    uda.insert("houseInfo", houseInfo)
                        .then(
                            () => {
                                uda.find("houseInfo", {
                                    id: 1,
                                    area: 1,
                                    floor: 1
                                }, null, null, 0, 10)
                                    .then(
                                        (result) => {
                                            expect(result).to.be.an("array");
                                            expect(result).to.have.length(1);
                                            expect(result[0].area).to.be.eql(houseInfo.area);
                                            expect(result[0].floor).to.be.eql(houseInfo.floor);

                                            uda.rollbackTransaction()
                                                .then(
                                                    () => {
                                                        uda.find("houseInfo", {
                                                                id: 1,
                                                                area: 1,
                                                                floor: 1
                                                            }, null, null, 0, 10)
                                                            .then(
                                                                (result) => {
                                                                    expect(result).to.be.an("array");
                                                                    expect(result).to.have.length(0);

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
                                    )
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


    it("[tt0.1] commit transaction in mysql", (done) => {
        uda.startTransaction("mysql")
            .then(
                () => {
                    const houseInfo = JSON.parse(JSON.stringify(houseInfos[0]));
                    uda.insert("houseInfo", houseInfo)
                        .then(
                            () => {
                                uda.find("houseInfo", {
                                        id: 1,
                                        area: 1,
                                        floor: 1
                                    }, null, null, 0, 10)
                                    .then(
                                        (result) => {
                                            expect(result).to.be.an("array");
                                            expect(result).to.have.length(1);
                                            expect(result[0].area).to.be.eql(houseInfo.area);
                                            expect(result[0].floor).to.be.eql(houseInfo.floor);

                                            uda.commitTransaction()
                                                .then(
                                                    () => {
                                                        uda.find("houseInfo", {
                                                                id: 1,
                                                                area: 1,
                                                                floor: 1
                                                            }, null, null, 0, 10)
                                                            .then(
                                                                (result) => {
                                                                    expect(result).to.be.an("array");
                                                                    expect(result).to.have.length(1);
                                                                    expect(result[0].area).to.be.eql(houseInfo.area);
                                                                    expect(result[0].floor).to.be.eql(houseInfo.floor);

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
                                    )
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


})
