/**
 * Created by Administrator on 2016/6/7.
 */
"use strict";


var expect = require("expect.js");

const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dataSource = require("./def/dataSource");
const schema3 = require("./def/schemas/schema3");
const schema4 = require("./def/schemas/schema4");
const now = Date.now();

const g_houseInfos = [
    {
        area: 145.4,
        floor: 1
    },
    {
        area: 67.0,
        floor: 6
    }
];

const g_houses = [
    {
        buildAt: now,
        status: "verifying"
    },
    {
        buildAt: now,
        status: "offline"
    }
];

const g_users = [
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

function initData(uda, users, houses, houseInfos) {
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
                                        uda.insert({
                                                name: "user",
                                                data: ele
                                            })
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
                                                    uda.insert({
                                                            name: "houseInfo",
                                                            data: ele
                                                        })
                                                        .then(
                                                            (result) => {
                                                                houses[index].houseInfoId = result.id || result._id;
                                                                return uda.insert({
                                                                        name: "house",
                                                                        data: houses[index]
                                                                    })
                                                                    .then(
                                                                        (hItem) => {
                                                                            houses[index] = hItem;
                                                                            let contract = {
                                                                                owner: users[index * 2],
                                                                                renter: users[index * 2 + 1],
                                                                                price: 2000
                                                                            };
                                                                            return uda.insert({
                                                                                    name: "contract",
                                                                                    data: contract
                                                                                })
                                                                                .then(
                                                                                    (result) => {
                                                                                        return uda.updateOneById({
                                                                                                name: "house",
                                                                                                data: {
                                                                                                    $set: {
                                                                                                        contract: result
                                                                                                    }
                                                                                                },
                                                                                                id: (hItem.id || hItem._id)
                                                                                            })
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

function checkResult1(result) {
    expect(result).to.be.an("object");
    expect(result.buildAt).to.eql(new Date(now));
    expect(result.status).to.eql("verifying");
    expect(result.contract.owner.name).to.eql("xiaoming");
    expect(result.contract.renter.name).to.eql("xiaohong");
}


describe("test select with joins in mysql 1", function () {

    this.timeout(10000);
    const houses = JSON.parse(JSON.stringify(g_houses));
    const users = JSON.parse(JSON.stringify(g_users));
    const houseInfos = JSON.parse(JSON.stringify(g_houseInfos));

    before((done) => {
        uda.connect(dataSource)
            .then(
                (result) => {

                    let _schema4 = JSON.parse(JSON.stringify(schema4));
                    uda.setSchemas(_schema4)
                        .then(
                            () => {
                                initData(uda, users, houses, houseInfos)
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
            buildAt: 1,
            status: 1,
            contract: {
                owner: {
                    name: 1,
                    age: 1
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
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house",
                projection,
                query,
                sort,
                indexFrom,
                count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(1);
                    checkResult1(result[0]);

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
            buildAt: 1,
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


        uda.findById({
                name: "house",
                projection,
                id: houses[0].id
            })
            .then(
                (result) => {
                    checkResult1(result);
                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("[ts0.2 orderBy]", (done) => {
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
                price: {
                    $ne: 2001
                }
            }
        };
        const projection = {
            _id: 1,
            buildAt: 1,
            status: 1,
            contract: {
                owner: {
                    name: 1
                },
                renter: {
                    name: 1
                }
            },
            houseInfo: {
                area: 1
            }
        };

        const sort = {
            houseInfo: {
                area: -1
            }
        }
        const indexFrom = 0, count = 2;


        let promises = [];
        promises.push(uda.find({
                name: "house",
                projection,
                query,
                sort,
                indexFrom,
                count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);
                    checkResult1(result[0]);

                    return Promise.resolve();
                },
                (err) => {
                    done(err);
                }
            ));

        const sort2 = {
            houseInfo: {
                area: 1
            }
        }
        promises.push(uda.find({
                name: "house",
                projection,
                query,
                sort: sort2,
                indexFrom,
                count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);
                    checkResult1(result[1]);

                    return Promise.resolve();
                },
                (err) => {
                    done(err);
                }
            ));

        Promise.all(promises)
            .then(
                () => {
                    done();
                },
                (err) => {
                    done(err);
                }
            );
    });

    it("[ts0.3 no result in joined table]", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 440
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
            buildAt: 1,
            status: 1,
            contractId: 1,
            contract: {
                ownerId: 1,
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
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house",
                projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(JSON.stringify(result));
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);
                    done();
                },
                (err) => {
                    done(err);
                }
            );
    });


    it("[ts0.4]test or on single table", (done) => {
        const query = {
            $or: [
                {buildAt: now},
                {status: "offline"}
            ]
        };
        const projection = {
            id: 1,
            buildAt: 1,
            status: 1
        };

        let sort;
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house",
                projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(result);
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("[ts0.5]test or on joined table", (done) => {
        const query = {
            houseInfo: {
                $or: [
                    {
                        floor: {
                            $eq: 1
                        }
                    },
                    {
                        area: {
                            $lt: 70
                        }
                    }
                ]
            }
        };
        const projection = {
            id: 1,
            buildAt: 1,
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

        let sort;
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house",
                projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(result);
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });

    it("[ts0.6]测试查询传null的现象", () => {
        const projection = {
            id: 1,
        };
        const where = {
            id: 1,
            buildAt: null
        };


        try {
            return uda.find({
                    name: "house",
                    projection,
                    query: where,
                    indexFrom: 0,
                    count: 10
                })
                .then(
                    (result) => {
                        throw new Error("不可能成功");
                    }
                );
        }
        catch (err) {
            console.log(err);
            return Promise.resolve();
        }
    });

    it("[ts0.10]", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 44
                }
            }
        };
        const projection = {
            id: 1,
            buildAt: 1,
            status: 1,
            contract: {
                owner: {
                    name: 1,
                    age: 1
                },
                renter: {
                    name: 1
                }
            },
            houseInfo: {
                area: 1
            }
        };

        const sort = {
            houseInfo: {
                area: 1
            }
        }
        const indexFrom = 0, count = 1;

        let update = {
            $set: {
                houseInfo: null
            }
        };
        uda.updateOneById({
                name: "house",
                data: update,
                id: houses[1].id
            })
            .then(
                () => {
                    uda.find({
                            name: "house", projection, query, sort, indexFrom, count
                        })
                        .then(
                            (result) => {
                                console.log(result);

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


    it("[ts0.20]test count", (done) => {
        const query = {
            $or: [
                {buildAt: now},
                {status: "offline"}
            ]
        };

        uda.count({
                name: "house",
                query
            })
            .then(
                (result) => {
                    console.log(result);
                    expect(result).to.be.an("object");
                    expect(result.count).to.eql(2);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("[ts0.31]test order with duplicate name", (done) => {
        const query = {
            buildAt: {
                $eq: now
            }
        };
        const projection = {
            id: 1,
            buildAt: 1,
            status: 1,
            contract: {
                owner: {
                    name: 1,
                    age: 1
                },
                renter: {
                    name: 1
                }
            },
            houseInfo: {
                area: 1
            }
        };

        const sort = {
            [uda.constants.createAtColumn]: -1
        }
        const indexFrom = 0, count = 1;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(result);

                    done();
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


describe("test select with joins in mongodb", function () {
    this.timeout(10000);

    const houses = JSON.parse(JSON.stringify(g_houses));
    const users = JSON.parse(JSON.stringify(g_users));
    const houseInfos = JSON.parse(JSON.stringify(g_houseInfos));

    before((done) => {
        uda.connect(dataSource)
            .then(
                (result) => {
                    let _schema4 = JSON.parse(JSON.stringify(schema4));
                    _schema4.house.source = "mongodb";
                    _schema4.houseInfo.source = "mongodb";
                    _schema4.contract.source = "mongodb";
                    _schema4.user.source = "mongodb";
                    uda.setSchemas(_schema4)
                        .then(
                            () => {
                                initData(uda, users, houses, houseInfos)
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
    });


    it("[ts1.0]", (done) => {
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
            _id: 1,
            buildAt: 1,
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
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(1);
                    checkResult1(result[0]);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("[ts1.1]", (done) => {
        const projection = {
            _id: 1,
            buildAt: 1,
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


        uda.findById({
                name: "house", projection, id: houses[0].id || houses[0]._id
            })
            .then(
                (result) => {
                    checkResult1(result);
                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("[ts1.2 orderBy]", (done) => {
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
                price: {
                    $ne: 2001
                }
            }
        };
        const projection = {
            _id: 1,
            buildAt: 1,
            status: 1,
            contract: {
                owner: {
                    name: 1
                },
                renter: {
                    name: 1
                }
            },
            houseInfo: {
                area: 1
            }
        };

        const sort = {
            houseInfo: {
                area: -1
            }
        }
        const indexFrom = 0, count = 2;


        let promises = [];
        promises.push(uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);
                    checkResult1(result[0]);

                    return Promise.resolve();
                },
                (err) => {
                    done(err);
                }
            ));

        const sort2 = {
            houseInfo: {
                area: 1
            }
        }
        promises.push(uda.find({
                name: "house", projection, query, sort: sort2, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);
                    checkResult1(result[1]);

                    return Promise.resolve();
                },
                (err) => {
                    done(err);
                }
            ));

        Promise.all(promises)
            .then(
                () => {
                    done();
                },
                (err) => {
                    done(err);
                }
            );
    });

    it("[ts1.3 no result in joined table]", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 440
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
            buildAt: 1,
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

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(result);
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);
                    done();
                },
                (err) => {
                    done(err);
                }
            );
    });


    it("[ts1.4]test or on single table", (done) => {
        const query = {
            $or: [
                {buildAt: now},
                {status: "offline"}
            ]
        };
        const projection = {
            id: 1,
            buildAt: 1,
            status: 1
        };

        let sort;
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(result);
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("[ts1.5]test or on joined table", (done) => {
        const query = {
            houseInfo: {
                $or: [
                    {
                        floor: {
                            $eq: 1
                        }
                    },
                    {
                        area: {
                            $lt: 70
                        }
                    }
                ]
            }
        };
        const projection = {
            id: 1,
            buildAt: 1,
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

        let sort;
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(result);
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });

    it("[ts1.10]", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 44
                }
            }
        };
        const projection = {
            id: 1,
            buildAt: 1,
            status: 1,
            contract: {
                owner: {
                    name: 1,
                    age: 1
                },
                renter: {
                    name: 1
                }
            },
            houseInfo: {
                area: 1
            }
        };

        const sort = {
            houseInfo: {
                area: -1
            }
        }
        const indexFrom = 0, count = 1;

        let update = {
            $set: {
                houseInfo: null
            }
        };
        uda.updateOneById({
                name: "house", data: update, id: houses[0]._id
            })
            .then(
                () => {
                    uda.find({
                            name: "house", projection, query, sort, indexFrom, count
                        })
                        .then(
                            (result) => {
                                console.log(result);

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


    it("[ts1.20]test count", (done) => {
        const query = {
            $or: [
                {buildAt: now},
                {status: "offline"}
            ]
        };

        uda.count({
                name: "house", query
            })
            .then(
                (result) => {
                    console.log(result);
                    expect(result).to.be.an("object");
                    expect(result.count).to.eql(2);

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

describe("test select with null in mysql 1", function () {

    const houses = JSON.parse(JSON.stringify(g_houses));
    const users = JSON.parse(JSON.stringify(g_users));
    const houseInfos = JSON.parse(JSON.stringify(g_houseInfos));


    this.timeout(10000);

    before((done) => {
        uda.connect(dataSource)
            .then(
                (result) => {
                    let _schema4 = JSON.parse(JSON.stringify(schema4));
                    uda.setSchemas(_schema4)
                        .then(
                            () => {
                                initData(uda, users, houses, houseInfos)
                                    .then(
                                        () => {
                                            // 把house 0 与 houseInfo 0的连接关系删除
                                            let house = {
                                                $set: {
                                                    houseInfo: null
                                                }
                                            }
                                            uda.updateOneById({
                                                    name: "house", data: house, id: houses[0].id
                                                })
                                                .then(
                                                    () => {
                                                        done();
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


    it("[ts2.0]", () => {
        const query = {
            buildAt: {
                $eq: now
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
            buildAt: 1,
            status: 1,
            contract: {
                owner: {
                    name: 1,
                    age: 1
                },
                renter: {
                    name: 1
                }
            },
            houseInfo: {
                area: 1,
                floor: 1
            }
        };

        const sort = {
            houseInfo: {
                area: 1
            }
        }
        const indexFrom = 0, count = 1;

        return uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(JSON.stringify(result));
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(1);
                    checkResult1(result[0]);

                    return Promise.resolve();
                }
            )
    });


    it("[ts2.1]test $between 1", (done) => {
        const query = {
            contract: {
                price: {
                    $between: {
                        $left: 1999,
                        $right: 2002,
                    }
                }
            }
        };
        const projection = {
            id: 1,
            buildAt: 1,
            status: 1,
            contract: {
                owner: {
                    name: 1,
                    age: 1
                },
                renter: {
                    name: 1
                }
            },
            houseInfo: {
                area: 1,
                floor: 1
            }
        };

        const sort = {
            houseInfo: {
                area: 1
            }
        };
        const indexFrom = 0, count = 1;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(result);
                    expect(result).to.be.an("array");
                    if (result.length === 1) {
                        // 这里根据mongodb的连接算法，先实行skip + count，再进行lookup，是有可能返回0行的
                        expect(result).to.have.length(1);
                        checkResult1(result[0]);
                    }
                    else {
                        expect(result).to.have.length(0);
                    }

                    done();
                },
                (err) => {
                    done(err);
                }
            );
    });


    it("[ts2.2] select with or", (done) => {
        const query = {
            $or: [
                {
                    buildAt: {
                        $eq: now
                    },
                },
                {
                    buildAt: now
                }
            ]
        };
        const projection = null;

        const sort = {
            houseInfo: {
                area: 1
            }
        };
        const indexFrom = 0, count = 1;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    console.log(result);
                    done();
                },
                (err) => {
                    done(err);
                }
            );
    });

    // 要支持这个需要对整体推倒重来，代价有点大啊  by Xc 20181230
    /*it('[ts2.3] 带join的or查询', () => {
        const query = {
            $or: [
                {
                    houseInfo: {
                        floor: 1,
                    },
                },
                {
                    contract: {
                        price: 1.1,
                    },
                }
            ],
        };
        const projection = null;

        const sort = {
            houseInfo: {
                area: 1
            }
        };
        const indexFrom = 0, count = 1;

        return uda.find({
                name: "house", projection, query, sort, indexFrom, count
            });
    })*/


    it("[ts2.4] select with for update", (done) => {
        const query = {
            id: 1
        };
        const projection = {id: 1};

        const indexFrom = 0, count = 1;
        uda.find({
            name: "house", projection, query, indexFrom, count, forUpdate: true
        }).then(
            (result)=>{
                console.log(result);
                done();
            },
            (err)=>{
                done(err);
            }
        )
    });
    describe("test select with null in mongodb 1", function () {

        const houses = JSON.parse(JSON.stringify(g_houses));
        const users = JSON.parse(JSON.stringify(g_users));
        const houseInfos = JSON.parse(JSON.stringify(g_houseInfos));
        this.timeout(8000);

        before((done) => {
            uda.connect(dataSource)
                .then(
                    (result) => {
                        let _schema4 = JSON.parse(JSON.stringify(schema4));
                        _schema4.house.source = "mongodb";
                        _schema4.houseInfo.source = "mongodb";
                        _schema4.contract.source = "mongodb";
                        _schema4.user.source = "mongodb";
                        uda.setSchemas(_schema4)
                            .then(
                                () => {
                                    initData(uda, users, houses, houseInfos)
                                        .then(
                                            () => {
                                                // 把house 0 与 houseInfo 0的连接关系删除
                                                let house = {
                                                    $set: {
                                                        houseInfo: null
                                                    }
                                                }
                                                uda.updateOneById({
                                                        name: "house", data: house, id: houses[0]._id
                                                    })
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


        it("[ts3.0]", (done) => {
            const query = {
                buildAt: {
                    $eq: now
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
                buildAt: 1,
                status: 1,
                contract: {
                    owner: {
                        name: 1,
                        age: 1
                    },
                    renter: {
                        name: 1
                    }
                },
                houseInfo: {
                    area: 1,
                    floor: 1
                }
            };

            const sort = {
                houseInfo: {
                    area: 1
                }
            }
            const indexFrom = 0, count = 1;

            uda.find({
                    name: "house", projection, query, sort, indexFrom, count
                })
                .then(
                    (result) => {
                        console.log(result);
                        expect(result).to.be.an("array");
                        if (result.length === 1) {
                            // 这里根据mongodb的连接算法，先实行skip + count，再进行lookup，是有可能返回0行的
                            expect(result).to.have.length(1);
                            checkResult1(result[0]);
                        }
                        else {
                            expect(result).to.have.length(0);
                        }

                        done();
                    },
                    (err) => {
                        done(err);
                    }
                );
        });

        it("[ts3.1] select with empty projection", (done) => {
            const query = {
                buildAt: {
                    $eq: now
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
            const projection = null;

            const sort = {
                houseInfo: {
                    area: 1
                }
            }
            const indexFrom = 0, count = 1;

            uda.find({
                    name: "house", projection, query, sort, indexFrom, count
                })
                .then(
                    (result) => {
                        console.log(result);
                        expect(result).to.be.an("array");
                        if (result.length === 1) {
                            // 这里根据mongodb的连接算法，先实行skip + count，再进行lookup，是有可能返回0行的
                            expect(result).to.have.length(1);
                            expect(result[0]).to.be.an("object");
                            expect(result[0].buildAt).to.eql(new Date(now));
                            expect(result[0].status).to.eql("verifying");
                        }
                        else {
                            expect(result).to.have.length(0);
                        }

                        done();
                    },
                    (err) => {
                        done(err);
                    }
                );
        });
    });

    after((done) => {
        uda.disconnect()
            .then(done);
    });
});

describe('test groupBy', function() {
    const houses = JSON.parse(JSON.stringify(g_houses));
    const users = JSON.parse(JSON.stringify(g_users));
    const houseInfos = JSON.parse(JSON.stringify(g_houseInfos));
    this.timeout(8000);

    before(() => {
        return uda.connect(dataSource)
            .then(
                (result) => {
                    let _schema4 = JSON.parse(JSON.stringify(schema4));
                    return uda.setSchemas(_schema4)
                        .then(
                            () => {
                                return initData(uda, users, houses, houseInfos);
                            }
                        );
                }
            ).then(
                () => Promise.resolve()
            );
    });

    /**
     *
     * select max(floor), id from houseInfo group by id;
     */
    it('[ts4.1]simple groupBy', () => {
        return uda.find({
            name: 'houseInfo',
            projection: {
                id: 1,
                $fnCall: {
                    $format: 'MAX(%s)',
                    $arguments: ['floor'],
                    $as: 'maxFloor',
                },
            },
            groupBy: {
                id: 1,
            }
        }).then(
            (result) => {
                console.log(JSON.stringify(result));
                return ;
            }
        );
    });

    /**
     *
     * select avg(c.price), count(c.price), o.name as username from contract c, user o where c.ownerId = o.id and o.age > 18;
     */
    it('[ts4.2]complicated groupBy', () => {
        return uda.find({
            name: 'contract',
            projection: {
                $fnCall: {
                    $format: 'AVG(%s)',
                    $arguments: ['price'],
                    $as: 'avg',
                },
                $fnCall2: {
                    $format: 'COUNT(%s)',
                    $arguments: ['price'],
                    $as: 'count',
                },
                owner: {
                    name: 'username',
                },
            },
            groupBy: {
                owner: {
                    name: 1,
                },
            },
            query: {
                owner: {
                    age: {
                        $gt: 8,
                    },
                },
            },
        }).then(
            (result) => {
                console.log(JSON.stringify(result));
                return ;
            }
        );
    });

    /**
     *
     * select avg(c.price) as avg, count(c.price) as count, o.name as username from contract c, user o where c.ownerId = o.id order by count asc limit 0, 10;
     */
    it('[ts4.3]complicated groupBy with sort', () => {
        return uda.find({
            name: 'contract',
            projection: {
                $fnCall: {
                    $format: 'AVG(%s)',
                    $arguments: ['price'],
                    $as: 'avg',
                },
                $fnCall2: {
                    $format: 'COUNT(%s)',
                    $arguments: ['price'],
                    $as: 'count',
                },
                owner: {
                    name: 'username',
                },
            },
            groupBy: {
                owner: {
                    name: 1,
                },
            },
            sort: {
                count: 2,
            },
            indexFrom: 0,
            count: 10,
        }).then(
            (result) => {
                console.log(JSON.stringify(result));
                return ;
            }
        );
    });

    after(() => {
        return uda.disconnect();
    });
});

describe('test left join empty', function() {

    const houses = JSON.parse(JSON.stringify(g_houses));
    const users = JSON.parse(JSON.stringify(g_users));
    const houseInfos = JSON.parse(JSON.stringify(g_houseInfos));
    this.timeout(8000);

    before(() => {
        return uda.connect(dataSource)
            .then(
                (result) => {
                    let _schema3 = JSON.parse(JSON.stringify(schema3));
                    return uda.setSchemas(_schema3)
                        .then(
                            () => {
                                const house = {
                                    buildAt: new Date('1990-01-01'),
                                    status: 'offline',
                                };

                                return uda.remove({
                                    name: 'house',
                                }).then(
                                    () => uda.insert({
                                        name: 'house',
                                        data: house
                                    })
                                );
                            }
                        );
                }
            ).then(
                () => Promise.resolve()
            );
    });

    it('[ts5.1] join empty', () => {
        return uda.find({
            name: 'house',
            indexFrom: 0,
            count: 1,
        }).then(
            (result) => {
                console.log(JSON.stringify(result));

                return Promise.resolve();
            }
        );
    })

})