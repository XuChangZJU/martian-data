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
    this.timeout(10000);
    const houses = JSON.parse(JSON.stringify(g_houses));
    const users = JSON.parse(JSON.stringify(g_users));
    const houseInfos = JSON.parse(JSON.stringify(g_houseInfos));

    before(() => {
        return uda.connect(dataSource)
            .then(
                (result) => {

                    let _schema4 = JSON.parse(JSON.stringify(schema4));
                    return uda.setSchemas(_schema4)
                        .then(
                            () => {
                                return initData(uda, users, houses, houseInfos)
                                    .then(
                                        () => {
                                            return;
                                        },
                                        (err) => {
                                            throw err;
                                        }
                                    );

                            }
                        );
                }
            );
    });

    it("{tsub0.1}", () => {
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

        return uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(2);

                    return;
                }
            )
    });

    it("{tsub0.2}", () => {
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

        return uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(1);

                    return
                }
            )
    });


    it("{tsub0.3}", () => {
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

        return uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);

                    return;
                }
            )
    });


    it("{tsub0.4}", () => {
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

        return uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);

                    return;
                }
            )
    });


    it("{tsub0.5}", () => {
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

        return uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);

                    return;
                }
            )
    });

    it("{tsub0.6}", () => {
        const now = Date.now();
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

        return uda.find({
                name: "house", projection, query, sort, indexFrom, count
            })
            .then(
                (result) => {
                    expect(result).to.be.an("array");
                    expect(result).to.have.length(0);

                    return;
                }
            )
    });

    it("{tsub0.7}", () => {
        const query = {
            contract: {
                price: {
                    $exists: true
                }
            },
            houseInfo: {
                id: {
                    $exists: true
                }
            },
            house: {
                id: {
                    $exists: true
                }
            },
            id: {
                $exists: true
            },
            $isql: {
                format: '${1} > ${2} + ${3} + ${4} + 5',
                $attrs: ['id'],
            },
        };
        query.$isql.$attrs.push(
            {
                $ref: "contract",
                $attr: 'price',
            },
            {
                $ref: "houseInfo",
                $attr: 'id',
            },
            {
                $ref: "house",
                $attr: 'id',
            }
        );

        //  这是个在同步函数中抛出的错误，需要try catch捕获
        try {
            return uda.find({
                    name: "house", query, projection: {id: 1}, indexFrom: 0, count: 10
                })
                .then(
                    (result) => {
                        throw new Error("不应当成功");
                    }
                )
        }
        catch (err) {
            console.log(err);
            return Promise.resolve();
        }
    });

    it("{tsub0.8}", () => {
        const query = {
            $isql: {
                format: '${1} < ${2} + 1',
                $attrs: ['id', 'buildAt'],
            },
            id: {
                $exists: true
            },
        };

        return uda.find({
                name: "house", query, projection: {id: 1}, indexFrom: 0, count: 10
            })
            .then(
                (result) => {
                    return;
                }
            )
    });

    it("{tsub0.9}", () => {
        const query = {
            contract: {
                price: {
                    $exists: true
                }
            },
            $isql: {
                format: '${1} > ${2} + ${3} + 5',
                $attrs: ['id'],
            },
            id: {
                $exists: true
            },
        };
        query.$isql.$attrs.push(
            {
                $ref: "contract",
                $attr: 'price',
            },
            {
                $ref: "houseInfo",
                $attr: 'id',
            }
        );

        try {
            return uda.find({
                    name: "house", query, projection: {id: 1}, indexFrom: 0, count: 10
                })
                .then(
                    (result) => {
                        throw new Error("不应当成功");
                    }
                )
        }
        catch (err) {
            console.log(err);
            return Promise.resolve();
        }
    });

    it("{tsub0.10}", () => {
        const query = {
            contract: {
                price: {
                    $exists: true
                }
            },
            $isql: {
                format: '${1} > ${2} + ${3} + 5',
                $attrs: ['id'],
            },
            id: {
                $exists: true
            },
        };
        query.$isql.$attrs.push(
            {
                $ref: "contract",
                $attr: 'price',
            }
        );

        try {
            return uda.find({
                    name: "house", query, projection: {id: 1}, indexFrom: 0, count: 10
                })
                .then(
                    (result) => {
                        throw new Error("不应当成功");
                    }
                )
        }
        catch (err) {
            console.log(err);
            return Promise.resolve();
        }
    });

    describe("[tdat]根据id的查询无视_deleteAt_选项", ()=> {
        it("{tdat1.0}本地的查询，‘外键’被删除之后，关联的查找依旧支持", () => {
            return uda.find({
                name: "house",
                indexFrom: 0,
                count: 10
            }).then(
                (houseList)=> {
                    expect(houseList.length).to.greaterThan(0);
                    return uda.removeOneById({
                        name: "contract",
                        id: houseList[0].contractId
                    }).then(
                        ()=> {
                            return uda.findById({
                                name: "contract",
                                id: houseList[0].contractId
                            }).then(
                                (contract)=> {
                                    expect(contract["_deleteAt_"]).not.to.be(null);
                                    return uda.findById({
                                        name: "house",
                                        projection: {
                                            id: 1,
                                            contractId: 1,
                                            contract: {
                                                id: 1,
                                                "_deleteAt_": 1
                                            }
                                        },
                                        id: houseList[0].id
                                    }).then(
                                        (houseInfo)=> {
                                            expect(houseInfo.contract["_deleteAt_"]).not.to.be(null);
                                        });
                                });
                        });
                });
        });

        it("{tdat1.1}本地的查询，本身被_deleteAt_时，根据id查找还可以查到", () => {
            return uda.find({
                name: "house",
                indexFrom: 0,
                count: 10
            }).then(
                (houseList)=> {
                    expect(houseList.length).to.greaterThan(0);
                    return uda.removeOneById({
                        name: "house",
                        id: houseList[0].id
                    }).then(
                        ()=> {
                            return uda.findById({
                                name: "house",
                                projection: {
                                    id: 1,
                                    contractId: 1,
                                    contract: {
                                        id: 1,
                                        "_deleteAt_": 1
                                    }
                                },
                                id: houseList[0].id
                            }).then(
                                (houseInfo)=> {
                                    expect(houseInfo["_deleteAt_"]).not.to.be(null);
                                    return uda.removeOneById({
                                        name: "contract",
                                        id: houseList[0].contractId
                                    }).then(
                                        ()=> {
                                            return uda.findById({
                                                name: "house",
                                                projection: {
                                                    id: 1,
                                                    contractId: 1,
                                                    contract: {
                                                        id: 1,
                                                        "_deleteAt_": 1
                                                    }
                                                },
                                                id: houseList[0].id
                                            }).then(
                                                (houseInfo)=> {
                                                    expect(houseInfo["_deleteAt_"]).not.to.be(null);
                                                    expect(houseInfo.contract["_deleteAt_"]).not.to.be(null);
                                                });
                                        });
                                });
                        });
                });
        });

        it("{tdat2.0}本地联合远端的查询，‘外键’被删除之后，关联的查找依旧支持", () => {
            return uda.insert({
                name: "contract",
                data: {
                    ownerId: 111,
                    renterId: 111,
                    price: 100
                }
            }).then(
                (contract)=> {
                    return uda.insert({
                        name: "remoteUser",
                        data: {
                            name: "wangyuef",
                            contractId: contract.id
                        }
                    }).then(
                        (remoteUser)=> {
                            return uda.removeOneById({
                                    name: "remoteUser",
                                    id: remoteUser.id
                                })
                                .then(
                                    ()=> {
                                        return uda.findById({
                                            name: "remoteUser",
                                            id: remoteUser.id
                                        });
                                    }
                                )
                                .then(
                                    (remoteUserInfo)=> {
                                        expect(remoteUserInfo["_deleteAt_"]).not.to.be(null);
                                        return uda.removeOneById({
                                            name: "contract",
                                            id: contract.id
                                        }).then(
                                            ()=> {
                                                return uda.findById({
                                                    name: "remoteUser",
                                                    projection: {
                                                        id: 1,
                                                        contractId: 1,
                                                        contract: {
                                                            id: 1,
                                                            "_deleteAt_": 1
                                                        }
                                                    },
                                                    id: remoteUser.id
                                                }).then(
                                                    (remoteUserInfo)=> {
                                                        expect(remoteUserInfo.contract["_deleteAt_"]).not.to.be(null);
                                                    });
                                            });
                                    });
                        });
                });
        });

        it("{tdat2.1}本地联合远端的查询，‘外键’被删除之后，关联的查找依旧支持", () => {
            return uda.insert({
                name: "contract",
                data: {
                    ownerId: 111,
                    renterId: 111,
                    price: 100
                }
            }).then(
                (contract)=> {
                    return uda.insert({
                        name: "remoteUser",
                        data: {
                            name: "wangyuef",
                            contractId: contract.id
                        }
                    }).then(
                        (remoteUser)=> {
                            return uda.removeOneById({
                                    name: "contract",
                                    id: contract.id
                                })
                                .then(
                                    ()=> {
                                        return uda.findById({
                                            name: "remoteUser",
                                            id: remoteUser.id
                                        });
                                    }
                                )
                                .then(
                                    (remoteUserInfo)=> {
                                        expect(remoteUserInfo["_deleteAt_"]).to.be(null);
                                        expect(remoteUserInfo.contract["_deleteAt_"]).not.to.be(null);
                                        return uda.removeOneById({
                                            name: "remoteUser",
                                            id: remoteUser.id
                                        }).then(
                                            ()=> {
                                                return uda.findById({
                                                    name: "remoteUser",
                                                    projection: {
                                                        id: 1,
                                                        contractId: 1,
                                                        contract: {
                                                            id: 1,
                                                            "_deleteAt_": 1
                                                        }
                                                    },
                                                    id: remoteUser.id
                                                }).then(
                                                    (remoteUserInfo)=> {
                                                        expect(remoteUserInfo.contract["_deleteAt_"]).not.to.be(null);
                                                    });
                                            });
                                    });
                        });
                });
        });
    });

    describe("[tino]测试$in算子支持子查询", ()=> {
        it("{tino1.0}测试$in算子，支持子查询（暂时只支持同源查询）", () => {
            return uda.remove({
                name: "contract"
            }).then(
                ()=> {
                    return uda.insert({
                        name: "contract",
                        data: {
                            ownerId: 111,
                            renterId: 111,
                            price: 100
                        }
                    }).then(
                        (contract)=> {
                            return uda.find({
                                name: "contract",
                                query: {
                                    id: {
                                        $in: {
                                            projection: "id",
                                            name: "contract",
                                            query: {
                                                id: {
                                                    $gte: 0
                                                },
                                                // 加上这个可以测query中有连接子树，但结果就是0行了。by Xc 20201105
                                                /*owner: {
                                                    name: {
                                                        $exists: true,
                                                    },
                                                },*/
                                                "_deleteAt_": {
                                                    $exists: false
                                                }
                                            }
                                        }
                                    },
                                    "_deleteAt_": {
                                        $exists: false,     // 用id作为query的条件，则不会过滤deleteAt的行
                                    }
                                },
                                indexFrom: 0,
                                count: 10
                            }).then(
                                (list)=>expect(list.length).to.equal(1)
                            );
                        });
                });
        });

        it("{tino1.1}测试$in算子，支持子查询（远端连接）", () => {
            return uda.remove({
                name: "contract"
            }).then(
                ()=> {
                    return uda.insert({
                        name: "contract",
                        data: {
                            ownerId: 111,
                            renterId: 111,
                            price: 100
                        }
                    }).then(
                        (contract)=> {
                            return uda.find({
                                name: "contract",
                                query: {
                                    id: {
                                        $in: `select id from house`
                                    },
                                    "_deleteAt_": {
                                        $exists: false
                                    }
                                },
                                indexFrom: 0,
                                count: 10
                            });
                        });
                });
        });
    })

});
