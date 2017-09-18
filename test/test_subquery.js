/**
 * Created by Administrator on 2016/8/18.
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
                                                                                                id: hItem.id || hItem._id
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
        );
}

describe("test subquery", function () {
    this.timeout(5000);
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

    it("{tsub0.1}", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 44
                }
            },
            $has: {
                name: "houseInfo",
                projection: {
                    area: 1
                },
                query: {
                    area: {
                        $gt: 44
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
                    expect(result).to.have.length(2);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });

    it("{tsub0.2}", (done) => {
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

        query.$hasnot = {
            name: "houseInfo",
            projection: {
                area: 1
            },
            query: {
                area: {
                    $gt: {
                        $ref: query.houseInfo,
                        $attr: "area"
                    }
                }
            }
        }
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
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(1);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("{tsub0.3}", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 70
                }
            }
        };

        query.$has = {
            name: "houseInfo",
            projection: {
                area: 1
            },
            query: {
                area: {
                    $gt: {
                        $ref: query.houseInfo,
                        $attr: "area"
                    }
                }
            }
        }
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
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("{tsub0.4}", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 70
                }
            }
        };

        query.$has = {
            name: "houseInfo",
            projection: {
                area: 1
            },
            query: {
                area: {
                    $gt: {
                        $ref: query.houseInfo,
                        $attr: "area"
                    }
                }
            }
        }
        query.$hasnot = {
            name: "houseInfo",
            projection: {
                area: 1
            },
            query: {
                area: {
                    $ref: query.houseInfo,
                    $attr: "area"
                }
            }
        }
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
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });


    it("{tsub0.5}", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 70
                }
            }
        };

        // 存在一个房屋面积小于之的，这个条件应该为True
        query.$has = {
            name: "houseInfo",
            projection: {
                area: 1
            },
            query: {
                area: {
                    $lt: {
                        $ref: query.houseInfo,
                        $attr: "area"
                    }
                }
            }
        };
        // 不存在一个房屋面积等于之的，这个条件加上则为false
        query.$hasnot = {
            name: "houseInfo",
            projection: {
                area: 1
            },
            query: {
                area: {
                    $ref: query.houseInfo,
                    $attr: "area"
                }
            }
        }
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
        const indexFrom = 0, count = 2;

        uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });

    it("{tsub0.6}", (done) => {
        const query = {
            buildAt: {
                $eq: now
            },
            houseInfo: {
                area: {
                    $gt: 70
                }
            }
        };

        query.$has = {
            name: "house",
            projection: {
                buildAt: 1
            },
            query: {
                buildAt: {
                    $lt: {
                        $ref: query,
                        $attr: "buildAt"
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
                area: 1
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
                    expect(result).to.have.length(0);

                    done();
                },
                (err) => {
                    done(err);
                }
            )
    });
})
