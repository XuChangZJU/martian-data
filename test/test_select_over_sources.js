/**
 * Created by Administrator on 2016/6/7.
 */
"use strict";



var expect = require("expect.js");


const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dataSource = require("./def/dataSource");
const schema5 = require("./def/schemas/schema5");
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

function checkResult1(result) {
    expect(result).to.be.an("object");
    expect(result.buildAt).to.eql(now);
    expect(result.status).to.eql("verifying");
    expect(result.contract.owner.name).to.eql("xiaoming");
    expect(result.contract.renter.name).to.eql("xiaohong");
}



describe("test select with joins over sources", function() {

    this.timeout(8000);

    before((done) => {
        uda.connect(dataSource)
            .then(
                (result) => {
                    let _schema5 = JSON.parse(JSON.stringify(schema5));
                    uda.setSchemas(_schema5)
                        .then(
                            () => {
                                initData(uda)
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

                            }
                        );
                },
                (err) => {
                    done(err);
                }
            );
    });


    it("[tsos0.0]", (done) => {
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
            id: 1,
            buildAt : 1,
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
        const indexFrom = 0, count = 2;

        try {
            uda.find("house", projection, query, sort, indexFrom, count)
                .then(
                    (result) => {
                        console.log(result);
                        done("跨源查询的sort算子落在非主表上但查询完成");
                    },
                    (err) => {
                        console.log(err);
                        done();
                    }
                );
        }
        catch(err) {
            console.log(err);
            done();
        }
    });


    it("[tsos0.1 get by id]", (done) => {

        const projection = {
            id: 1,
            buildAt : 1,
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
        uda.findById("house", projection, houses[0].id || houses[0]._id)
            .then(
                (result) => {
                    expect(result).to.be.an("object");
                    expect(result.houseInfo.area).to.eql(houseInfos[0].area);
                    expect(result.buildAt).to.eql(houses[0].buildAt);

                    done();
                },
                (err) => {
                    done(err);
                }
            );
    });

    it("[tsos0.2]", (done) => {
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
            buildAt : 1,
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
        const indexFrom = 0, count = 2;

        let update = {
            $set: {
                houseInfo: null
            }
        };
        uda.updateOneById("house", update, houses[0].id)
            .then(
                () => {
                    try {
                        uda.find("house", projection, query, sort, indexFrom, count)
                            .then(
                                (result) => {
                                    done("跨源查询的sort算子落在非主表上但查询完成");
                                }
                            )
                            .catch(
                                (err) => {
                                    console.log(err);
                                    done();
                                }
                            );
                    }
                    catch(err) {
                        console.log(err);
                        done();
                    }
                },
                (err) => {
                    done(err);
                }
            );
    });


    /*   it("[ts0.1]", (done) => {
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


     uda.findById("house", projection, houses[0].id)
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
     buildAt : 1,
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
     const indexFrom = 0, count = 1;


     let promises = [];
     promises.push(uda.find("house", projection, query, sort, indexFrom, count)
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
     promises.push(uda.find("house", projection, query, sort2, indexFrom, count)
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
     expect(result).to.be.an("array");
     expect(result).to.have.length(0);
     done();
     },
     (err) => {
     done(err);
     }
     );
     });*/



    after((done) => {
        uda.disconnect()
            .then(done);
    });
});

describe("test left joins over sources", function() {

    this.timeout(8000);

    before(() => {
        return uda.connect(dataSource)
    });

    it("[tleftos1.0]", () => {
        return uda.connect(dataSource)
            .then(
                (result) => {
                    let _schema5 = JSON.parse(JSON.stringify(schema5));
                    return uda.setSchemas(_schema5)
                        .then(
                            () => {
                                return uda.dropSchemas()
                                    .then(
                                        () => {
                                            return uda.createSchemas()
                                                .then(
                                                    () => {
                                                        return uda.insert("house", {
                                                                buildAt: now,
                                                                status: "verifying"
                                                            })
                                                            .then(
                                                                () => {
                                                                    return uda.find("house", {
                                                                            buildAt: 1,
                                                                            houseInfo: {
                                                                                area: 1,
                                                                                floor: 1
                                                                            }
                                                                        }, null, null, 0, 100)
                                                                        .then(
                                                                            (results) => {
                                                                                expect(results).to.have.length(1);
                                                                                expect(results[0].houseInfo).to.eql(null);
                                                                                return Promise.resolve();
                                                                            }
                                                                        );
                                                                }
                                                            );
                                                    }
                                                );
                                        }
                                    );
                            }
                        );
                }
            );
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
            id: 1,
            buildAt : 1,
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
        const indexFrom = 0, count = 2;

        try {
            uda.find("house", projection, query, sort, indexFrom, count)
                .then(
                    (result) => {
                        console.log(result);
                        done("跨源查询的sort算子落在非主表上但查询完成");
                    },
                    (err) => {
                        console.log(err);
                        done();
                    }
                );
        }
        catch(err) {
            console.log(err);
            done();
        }
    });



    after(() => {
        return uda.disconnect();
    });
});



