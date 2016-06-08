/**
 * Created by Administrator on 2016/6/7.
 */
"use strict";



var expect = require("expect.js");


const uda = require("../src/UnifiedDataAccess");
const dataSource = require("./def/dataSource");
const schema3 = require("./def/schemas/schema3");
const schema4 = require("./def/schemas/schema4");
const now = Date.now();

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
        age: 24
    },
    {
        name: "xiaohong",
        age: 21
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

describe("test select with joins in mysql & mongodb", function() {

    this.timeout(5000);

    before((done) => {
        uda.connect(dataSource)
            .then(
                (result) => {
                    uda.setSchemas(schema4);
                    uda.dropSchemas()
                        .then(
                            () => {
                                uda.createSchemas()
                                    .then(
                                        () => {
                                            let promises = [];
                                            users.forEach(
                                                (ele, idx) => {
                                                    const idx2 = idx;
                                                    promises.push(uda.insert("user", ele)
                                                        .then(
                                                            (result) => {
                                                                users[idx2].id = result.id;
                                                                return Promise.resolve();
                                                            },
                                                            (err) => {
                                                                return Promise.reject(err);
                                                            }
                                                        ));
                                                }
                                            )
                                            Promise.all(promises)
                                                .then(
                                                    ()=> {
                                                        let promises2 = [];
                                                        houseInfos.forEach(
                                                            (ele, index) => {
                                                                promises2.push(
                                                                    uda.insert("houseInfo", ele)
                                                                        .then(
                                                                            (result) => {
                                                                                houses[index].houseInfoId = result.id;
                                                                                return uda.insert("house", houses[index])
                                                                                    .then(
                                                                                        (hItem) => {
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
                                                                                                        }, hItem.id)
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
                                                        Promise.all(promises2)
                                                            .then(
                                                                () => {
                                                                    done()
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


    it("[ts0.0]", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 44
                }
            },
            contract: {
                owner: {
                    name: "xiaoming"
                },
                renter: {
                    name: "xiaohong"
                },
                price: {
                    $ne: 2001
                }
            }
        };
        const projection = {
            id: 1,
            buildAt : 1,
            status: 1,
            contract: {
                owner: {
                    name: 1
                },
                renter: {
                    name: 1
                }
            }
        };

        const sort = {
            houseInfo: {
                area: 1
            }
        }
        const indexFrom = 0, count = 1;

        uda.find("house", projection, query, sort, indexFrom, count)
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


    it("[ts0.1]", (done) => {
        const projection = {
            id: 1,
            buildAt : 1,
            status: 1,
            contract: {
                owner: {
                    name: 1
                },
                renter: {
                    name: 1
                }
            }
        };


        uda.findById("house", projection, 1)
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



    after((done) => {
        uda.disconnect()
            .then(done);
    });
});





describe("test select with joins in mysql", () => {
    before((done) => {
        uda.connect(dataSource)
            .then(
                (result) => {
                    uda.setSchemas(schema3);
                    uda.dropSchemas()
                        .then(
                            () => {
                                uda.createSchemas()
                                    .then(
                                        () => {
                                            let promises = [];
                                            houseInfos.forEach(
                                                (ele, index) => {
                                                    promises.push(
                                                        uda.insert("houseInfo", ele)
                                                            .then(
                                                                (result) => {
                                                                    houses[index].houseInfoId = result.id;
                                                                    uda.insert("house", houses[index])
                                                                        .then(
                                                                            () => {
                                                                                return Promise.resolve();
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
                                            Promise.all(promises)
                                                .then(
                                                    () => {
                                                        done()
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
            );
    });


    it("[ts1.0]", (done) => {
        const query = {
            buildAt: {
                $gt: now
            },
            houseInfo: {
                area: {
                    $gt: 44
                }
            }
        };
        const projection = {
            buildAt : 1,
            status: 1
        };

        const sort = {
            houseInfo: {
                area: 1
            }
        }
        const indexFrom = 0, count = 1;

        let result = uda.find("house", projection, query, sort, indexFrom, count);
        console.log(result);

        done();
    });



    after((done) => {
        uda.disconnect()
            .then(done);
    });
});
