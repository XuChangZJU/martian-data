/**
 * Created by Administrator on 2016/6/15.
 */
"use strict";


var expect = require("expect.js");


const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dataSource3 = require("./def/dataSource2");
const dataSource2 = require("./def/dataSource2");
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
                                        uda.insert({
                                                name: "user", data: ele
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
                                                            name: "houseInfo", data: ele
                                                        })
                                                        .then(
                                                            (result) => {
                                                                houses[index].houseInfo = result;
                                                                return uda.insert({
                                                                        name: "house", data: houses[index]
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
                                                                                    name: "contract", data: contract
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
        )
}


describe("test transaction", function () {
    this.timeout(5000);
    before((done) => {
        uda.connect(dataSource3)
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
                (txn) => {
                    const houseInfo = JSON.parse(JSON.stringify(houseInfos[0]));
                    uda.insert({
                            name: "houseInfo", data: houseInfo, txn
                        })
                        .then(
                            () => {
                                uda.find({
                                        name: "houseInfo",
                                        projection: {
                                            id: 1,
                                            area: 1,
                                            floor: 1
                                        },
                                        indexFrom: 0,
                                        count: 10,
                                        txn
                                    })
                                    .then(
                                        (result) => {
                                            expect(result).to.be.an("array");
                                            expect(result).to.have.length(1);
                                            expect(result[0].area).to.be.eql(houseInfo.area);
                                            expect(result[0].floor).to.be.eql(houseInfo.floor);

                                            uda.rollbackTransaction(txn)
                                                .then(
                                                    () => {
                                                        uda.find({
                                                                name: "houseInfo",
                                                                projection: {
                                                                    id: 1,
                                                                    area: 1,
                                                                    floor: 1
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
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
                (txn) => {
                    const houseInfo = JSON.parse(JSON.stringify(houseInfos[0]));
                    uda.insert({
                            name: "houseInfo", data: houseInfo, txn
                        })
                        .then(
                            () => {
                                uda.find({
                                        name: "houseInfo",
                                        projection: {
                                            id: 1,
                                            area: 1,
                                            floor: 1
                                        },
                                        indexFrom: 0,
                                        count: 10,
                                        txn
                                    })
                                    .then(
                                        (result) => {
                                            expect(result).to.be.an("array");
                                            expect(result).to.have.length(1);
                                            expect(result[0].area).to.be.eql(houseInfo.area);
                                            expect(result[0].floor).to.be.eql(houseInfo.floor);

                                            uda.commitTransaction(txn)
                                                .then(
                                                    () => {
                                                        uda.find({
                                                                name: "houseInfo",
                                                                projection: {
                                                                    id: 1,
                                                                    area: 1,
                                                                    floor: 1
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            })
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

});

describe('test concurrent transaction in mysql', function () {
    this.timeout(15000);
    const uda2 = new UDA();
    before(() => {
        return uda2.connect(dataSource2)
            .then(
                () => {
                    const _schema = JSON.parse(JSON.stringify(schema));
                    return uda2.setSchemas(_schema)
                        .then(
                            () => {
                                return uda2.dropSchemas()
                                    .then(
                                        () => {
                                            return uda2.createSchemas();
                                        }
                                    );
                            }
                        );
                }
            );
    });

    it("[tct1.1] recursive transaction in mysql", () => {
        // 因为connectionPool中写的连接数目大于2，所以这个测试能过，如果是1个连接就过不了。这种写法仍然不推荐
        return uda2.startTransaction("mysql")
            .then(
                (txn1) => {
                    return uda2.startTransaction('mysql')
                        .then(
                            (txn2) => {
                                return uda2.commitTransaction(txn1)
                                    .then(
                                        () => uda2.commitTransaction(txn2)
                                    )
                            }
                        )
                }
            );
    });


    it("[tct1.2] concurrent transaction in mysql", () => {
        let areaValue;
        const f = (houseInfoId, option) => {
            return uda2.startTransaction('mysql', option)
                .then(
                    (txn) => {
                        return uda2.findById({
                                name: 'houseInfo',
                                projection: {
                                    area: 1
                                },
                                id: houseInfoId,
                                txn
                            })
                            .then(
                                (houseInfo) => {
                                    return uda2.updateOneById({
                                            name: 'houseInfo',
                                            data: {
                                                $set: {
                                                    area: houseInfo.area + 1,
                                                },
                                            },
                                            id: houseInfoId,
                                            txn
                                        })
                                        .then(
                                            () => {
                                                return uda2.commitTransaction(txn, {
                                                        isolationLevel: 'REPEATED READ',
                                                    })
                                                    .then(
                                                        () => {
                                                            areaValue++;
                                                            return Promise.resolve();
                                                        }
                                                    )
                                            }
                                        )
                                }
                            )
                            .catch(
                                (err) => {
                                    return uda2.rollbackTransaction(txn);
                                }
                            );
                    }
                );
        };

        return uda2.insert({
            name:'houseInfo',
            data:{
                floor: 1,
                area: 100
            }
        })
            .then(
                (houseInfo) => {
                    areaValue = 100;
                    const promises = [];
                    for (let i = 0; i < 100; i++) {
                        promises.push(f(houseInfo.id));
                    }
                    return Promise.all(promises)
                        .then(
                            () => {
                                return uda2.findById({
                                    name:'houseInfo',
                                    projection: {
                                        area: 1,
                                    },
                                    id: houseInfo.id
                                })
                                    .then(
                                        (houseInfo2) => {
                                            // 不使用串行隔离级别，得到的结果不可控
                                            console.log(`value in db ${houseInfo2.area}, value outside: ${areaValue}`);
                                            areaValue = houseInfo2.area;

                                            const promises2 = [];
                                            for (let i = 0; i < 100; i++) {
                                                promises2.push(f(houseInfo.id, {
                                                    isolationLevel: 'SERIALIZABLE'
                                                }));
                                            }

                                            const checkValue = () => {
                                                return uda2.findById({
                                                    name:'houseInfo',
                                                        projection:{
                                                            area: 1,
                                                        },
                                                        id:houseInfo.id
                                                })
                                                    .then(
                                                        (houseInfo3) => {
                                                            console.log(`[serializable]value in db ${houseInfo3.area}, value outside: ${areaValue}`);
                                                            expect(houseInfo3.area).to.eql(areaValue);
                                                            return Promise.resolve();
                                                        }
                                                    );
                                            }
                                            return Promise.all(promises2)
                                                .then(
                                                    checkValue
                                                )
                                                .catch(
                                                    checkValue
                                                );
                                        }
                                    );
                            }
                        );
                }
            );
    });


    it("[tct1.3] show variables", () => {
        return uda2.getSource('mysql')
            .execSql("show variables like '%iso%'")
            .then(
                (result) => {
                    console.log(result[0].Value);
                    return Promise.resolve();
                }
            )
    });


    it("[tct1.4]batch insert in mysql with txn may have a bug", () => {
        let _schema = JSON.parse(JSON.stringify(schema));

        return uda2.setSchemas(_schema)
            .then(
                () => {
                    const items = [];
                    let now = Date.now();
                    const threadCount = 3;
                    for (let i = 0; i < threadCount; i++) {
                        items.push(
                            [
                                {
                                    buildAt: now++,
                                    status: "free"
                                },
                                {
                                    buildAt: now++,
                                    status: "offline"
                                },
                                {
                                    buildAt: now++,
                                    status: "free"
                                },
                                {
                                    buildAt: now++,
                                    status: "offline"
                                }
                            ]
                        );
                    }

                    const doInsert = (index) => {
                        return uda2.startTransaction('mysql', {
                            isolationLevel: 'SERIALIZABLE',
                        }).then(
                            (txn) => {
                                return uda2.insert({name: "house", data: items[index], txn}).then(
                                    (result) => {
                                        return uda2.commitTransaction(txn, {
                                            isolationLevel: 'REPEATABLE READ',
                                        })
                                    }
                                )
                            }
                        );
                    };

                    const i = (index) => {
                        if (index === threadCount) {
                            return Promise.resolve();
                        }
                        return doInsert(index)
                            .then(
                                () => i(index + 1)
                            );
                    };


                    return uda2.remove({name: 'house'}).then(
                        () => {
                            return i(0).then(
                                () => {
                                    return uda2.count(
                                        {
                                            name:'house'
                                        }
                                    ).then(
                                        (count) => {
                                            expect(count.count).to.eql(threadCount * 4);
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

    /**
     * Created by Administrator on 2017/2/16.
     */
    Promise.every = (promises) => {
        const result = promises.map(
            () => false
        );
        const promises2 = promises.map(
            (ele, idx) =>
                ele.then(
                    (res) => {
                        result[idx] = true;
                        return Promise.resolve(res);
                    }
                ).catch(
                    (err) => {
                        return Promise.resolve(err);
                    }
                )
        );

        return Promise.all(promises2)
            .then(
                (res) => {
                    const failure = result.findIndex(
                        (ele) => ele === false
                    );
                    if (failure !== -1) {
                        throw res[failure];
                    }
                    return Promise.resolve(res);
                }
            );
    };


    it("[tct1.5]事务并发执行多个任务，产生回滚", () => {
        const t = (houseInfo) => {
            let areaValue = 100;
            return uda2.startTransaction('mysql', {
                isolationLevel: 'SERIALIZABLE',
            }).then(
                (txn) => {
                    const f1 = () => {
                        return uda2.findById({
                                name: 'houseInfo',
                                projection: {
                                    area: 1
                                },
                                id: houseInfo.id,
                                txn
                            })
                            .then(
                                (houseInfo2) => {
                                    return uda2.updateOneById({
                                        name: 'houseInfo',
                                        data: {
                                            $set: {
                                                area: houseInfo2.area + 1,
                                            },
                                        },
                                        id: houseInfo.id,
                                        txn
                                    })
                                }
                            );
                    };
                    const f2 = () => {
                        return uda2.findById({
                            name:'houseInfo',
                                projection:{
                                    area: 1
                                },
                                id: houseInfo.id,
                                txn
                        })
                            .then(
                                (houseInfo2) => {
                                    return uda2.updateOneById({
                                        name:'houseInfo',
                                        data:{
                                            $set: {
                                                area: houseInfo2.area - 5,
                                            },
                                        },
                                        id:houseInfo.id,
                                        txn
                                    })
                                        .then(
                                            () => {
                                                throw new Error("我偏要失败");
                                            }
                                        )
                                }
                            );
                    };

                    const checkValue = () => {
                        return uda2.findById({
                            name:'houseInfo',
                                projection:{
                                    area: 1,
                                },
                                id:houseInfo.id 
                        })
                            .then(
                                (houseInfo3) => {
                                    console.log(`[serializable]value in db ${houseInfo3.area}, value outside: ${areaValue}`);
                                    expect(houseInfo3.area).to.eql(areaValue);
                                    return Promise.resolve();
                                }
                            );
                    };
                    return Promise.all([f2(), f1()])
                        .then(
                            () => {
                                console.log("我提交了");
                                return uda2.commitTransaction(txn, {
                                    isolationLevel: 'REPEATABLE READ',
                                }).then(
                                    () => checkValue()
                                )
                            }
                        )
                        .catch(
                            (err) => {
                                console.error(err);
                                console.log("我回滚了");
                                return uda2.rollbackTransaction(txn, {
                                    isolationLevel: 'REPEATABLE READ',
                                }).then(
                                    () => checkValue()
                                )
                            }
                        );
                }
            );
        };
        return uda2.insert({
    name: 'houseInfo', data: {
            floor: 1,
                area: 100
        }
}).then(
            (houseInfo) => {
                return uda2.insert({
                    name: 'houseInfo',
                    data: {
                        floor: 1,
                        area: 100
                    }
                }).then(
                    (houseInfo2) => {
                        return Promise.all(
                            [t(houseInfo), t(houseInfo2)]
                        );
                    }
                );
            }
        );
    });
})
