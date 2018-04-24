/**
 * Created by Administrator on 2016/6/22.
 */
"use strict";

let childProcess = require("child_process");
let spawn = childProcess.spawn;


let expect = require("expect.js");
let assert = require("assert");

const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dsRemote = require("./testRemote/dataSources/dsRemote");
const schemaRemote1 = require("./testRemote/schemas/schemaRemote1");
const schemaRemote2 = require("./testRemote/schemas/schemaRemote2");

const merge = require("lodash/merge");
const keys = require("lodash/keys");


function init(schemaRemote) {
    // 1.开一个进程先启动远程服务器
    // 这里如果加上"--debug-brk"参数，可以调远程服务器！！！ by xc
    let server = spawn("node", [/*"--debug-brk", */ "./test/testRemote/server/app.js"], {
        stdio: [0, 1, 2, 'ipc']
    });

    return new Promise(
        (resolve, reject) => {
            server.on("message", (msg) => {
                    if (msg.success) {
                        //2. 得到远端的Schema的属性定义，用之来初始化本地的Schema
                        uda.connect(dsRemote)
                            .then(
                                () => {
                                    let init = {
                                        method: "POST",
                                        headers: {
                                            "Content-type": "application/json"
                                        }
                                    }
                                    return uda.getSource("remote").getSchemas()
                                        .then(
                                            (result) => {
                                                // 3. 设置本地的Schema
                                                let schema = merge({}, result, schemaRemote);
                                                return uda.setSchemas(schema)
                                                    .then(
                                                        () => {
                                                            return uda.dropSchemas()
                                                                .then(
                                                                    () => {
                                                                        return uda.createSchemas()
                                                                            .then(
                                                                                () => {
                                                                                    resolve(server);
                                                                                    ;
                                                                                }
                                                                            );
                                                                    }
                                                                )
                                                        }
                                                    );
                                            }
                                        )
                                }
                            )
                            .catch(
                                (err) => {
                                    reject(err);
                                }
                            )
                    }
                    else {
                        reject(msg.error);
                    }
                }
            );


            server.on("error", (err) => {
                console.log('error');
                reject(err);
            })
        }
    )
}

describe("test remote 1", function () {
    this.timeout(5000);
    let server;
    before(() => {
        return init(schemaRemote1)
            .then((serv)=> {
                server = serv;
            });
    });

    it("[tre0.0]", () => {
        // 尝试插入数据，再查询
        return uda.insert({
                name: "user",
                data: {
                    name: "xc",
                    age: 33
                }
            })
            .then(
                (row) => {
                    let id = row.hasOwnProperty("id") ? row.id : row._id;

                    return uda.findById({
                            name: "user",
                            projection: {
                                name: 1,
                                age: 1
                            },
                            id
                        })
                        .then(
                            (result) => {
                                expect(result).to.be.an("object");
                                expect(result.name).to.eql("xc");
                                expect(result.age).to.eql(33);
                            }
                        );
                }
            )
    });

    it("[tre0.1]", () => {
        // 尝试插入数据，再查询
        return uda.insert({
                name: "user",
                data: {
                    name: "xc",
                    age: 33
                }
            })
            .then(
                (row) => {

                    let account = {
                        owner: row,
                        deposit: 10000
                    };
                    return uda.insert({
                            name: "account",
                            data: account
                        })
                        .then(
                            (row2) => {
                                let id = row2.hasOwnProperty("id") ? row2.id : row2._id;
                                return uda.findById({
                                        name: "account",
                                        projection: {
                                            owner: {
                                                name: 1,
                                                age: 1
                                            },
                                            deposit: 1
                                        },
                                        id
                                    })
                                    .then(
                                        (result) => {
                                            expect(result).to.be.an("object");
                                            expect(result.deposit).to.eql(10000);
                                            expect(result.owner).to.be.an("object");
                                            expect(result.owner.name).to.eql("xc");
                                            expect(result.owner.age).to.eql(33);
                                        }
                                    );
                            }
                        );
                }
            )
    });

    it("[tre0.2]", () => {
        // 尝试增删改查
        return uda.insert({
                name: "user",
                data: {
                    name: "xc",
                    age: 33
                }
            })
            .then(
                (row) => {
                    let id = row.hasOwnProperty("id") ? row.id : row._id;

                    return uda.findById({
                            name: "user",
                            projection: {
                                name: 1,
                                age: 1
                            },
                            id
                        })
                        .then(
                            (result) => {
                                expect(result).to.be.an("object");
                                expect(result.name).to.eql("xc");
                                expect(result.age).to.eql(33);

                                return uda.updateOneById({
                                        name: "user",
                                        data: {
                                            $set: {
                                                age: 34
                                            }
                                        },
                                        id
                                    })
                                    .then(
                                        () => {
                                            return uda.findById({
                                                    name: "user",
                                                    projection: {
                                                        name: 1,
                                                        age: 1
                                                    },
                                                    id
                                                })
                                                .then(
                                                    (result) => {
                                                        expect(result).to.be.an("object");
                                                        expect(result.name).to.eql("xc");
                                                        expect(result.age).to.eql(34);

                                                        return uda.removeOneById({
                                                                name: "user",
                                                                id
                                                            })
                                                            .then(
                                                                () => {
                                                                    return uda.findById({
                                                                            name: "user",
                                                                            projection: {
                                                                                name: 1,
                                                                                age: 1,
                                                                                _deleteAt_: 1
                                                                            },
                                                                            id
                                                                        })
                                                                        .then(
                                                                            (result) => {
                                                                                //  根据id查找可以无视_deleteAt_
                                                                                assert(result._deleteAt_);
                                                                            }
                                                                        );
                                                                }
                                                            )
                                                    }
                                                );
                                        }
                                    );
                            }
                        );
                }
            )
    });

    after(() => {
        server.kill("SIGTERM");
        // server.on("exit", done);
    });
});


describe("test remote 2", function () {
    this.timeout(5000);
    let server;
    before(() => {
        return init(schemaRemote2)
            .then((serv)=> {
                server = serv;
            })

    });

    beforeEach(
        ()=> {
            //  清空缓存
            keys(uda.connections).forEach(
                (connectionIdx)=> {
                    if (uda.connections[connectionIdx] && uda.connections[connectionIdx].mtStorage) {
                        uda.connections[connectionIdx].mtStorage.clearStorage();
                    }
                }
            )
        }
    )


    it("[tre1.0]", () => {
        let now = new Date();
        // 尝试插入数据，再进行连接查询
        return uda.insert({
                name: "user",
                data: {
                    name: "xc",
                    age: 33
                }
            })
            .then(
                (row) => {
                    let account = {
                        owner: row,
                        deposit: 10000
                    };
                    return uda.insert({
                            name: "account",
                            data: account
                        })
                        .then(
                            (row2) => {
                                return uda.insert({
                                        name: "order",
                                        data: {
                                            account: row2,
                                            time: now
                                        }
                                    })
                                    .then(
                                        (row3) => {
                                            let projection = {
                                                account: {
                                                    owner: {
                                                        name: 1,
                                                        age: 1
                                                    },
                                                    deposit: 1
                                                },
                                                time: 1
                                            }, query = {
                                                account: {
                                                    owner: {
                                                        age: {
                                                            $gt: 20
                                                        }
                                                    }
                                                }
                                            };
                                            return uda.find({
                                                    name: "order",
                                                    projection,
                                                    query,
                                                    indexFrom: 0,
                                                    count: 10
                                                })
                                                .then(
                                                    (result) => {
                                                        expect(result).to.be.an("array");
                                                        expect(result).to.have.length(1);
                                                        expect(result[0].time).to.eql(now);

                                                        // 加上count查询
                                                        return uda.count({
                                                                name: "order",
                                                                query: {time: now}
                                                            })
                                                            .then(
                                                                (result2) => {
                                                                    expect(result2).to.be.an("object");
                                                                    expect(result2.count).to.be.eql(1);
                                                                }
                                                            )
                                                    }
                                                )
                                        }
                                    )
                            }
                        );
                }
            )
    });

    it("[tre0.2]", () => {
        // 尝试增删改查
        return uda.insert({
                name: "user",
                data: {
                    name: "xc",
                    age: 33
                }
            })
            .then(
                (row) => {
                    let id = row.hasOwnProperty("id") ? row.id : row._id;

                    return uda.findById({
                            name: "user",
                            projection: {
                                name: 1,
                                age: 1
                            },
                            id
                        })
                        .then(
                            (result) => {
                                expect(result).to.be.an("object");
                                expect(result.name).to.eql("xc");
                                expect(result.age).to.eql(33);

                                return uda.updateOneById({
                                        name: "user",
                                        data: {
                                            $set: {
                                                age: 34
                                            }
                                        },
                                        id
                                    })
                                    .then(
                                        () => {
                                            return uda.findById({
                                                    name: "user",
                                                    projection: {
                                                        name: 1,
                                                        age: 1
                                                    },
                                                    id
                                                })
                                                .then(
                                                    (result) => {
                                                        expect(result).to.be.an("object");
                                                        expect(result.name).to.eql("xc");
                                                        expect(result.age).to.eql(34);

                                                        return uda.removeOneById({
                                                                name: "user",
                                                                id
                                                            })
                                                            .then(
                                                                () => {
                                                                    return uda.findById({
                                                                            name: "user",
                                                                            projection: {
                                                                                name: 1,
                                                                                age: 1,
                                                                                _deleteAt_: 1
                                                                            },
                                                                            id
                                                                        })
                                                                        .then(
                                                                            (result) => {
                                                                                assert(result._deleteAt_);
                                                                            }
                                                                        );
                                                                }
                                                            )
                                                    }
                                                );
                                        }
                                    );
                            }
                        );
                }
            )
    });

    it("[tre0.3]测试mtStorage，带着useCache进行查询时，不会实时读取远端数据", () => {
        // 尝试增删改查
        return uda.insert({
                name: "user",
                data: {
                    name: "wangyuef",
                    age: 25
                }
            })
            .then(
                (row) => {
                    let id = row.hasOwnProperty("id") ? row.id : row._id;

                    return uda.findById({
                            name: "user",
                            projection: {
                                name: 1,
                                age: 1
                            },
                            useCache: true,
                            id
                        })
                        .then(
                            (result) => {
                                expect(result).to.be.an("object");
                                expect(result.name).to.eql("wangyuef");
                                expect(result.age).to.eql(25);

                                return uda.updateOneById({
                                        name: "user",
                                        data: {
                                            $set: {
                                                age: 26
                                            }
                                        },
                                        id
                                    })
                                    .then(
                                        () => {
                                            //  缓存查询还为更改前的值
                                            return uda.findById({
                                                    name: "user",
                                                    projection: {
                                                        name: 1,
                                                        age: 1
                                                    },
                                                    useCache: true,
                                                    id
                                                })
                                                .then(
                                                    (result) => {
                                                        expect(result).to.be.an("object");
                                                        expect(result.name).to.eql("wangyuef");
                                                        expect(result.age).to.eql(25);
                                                        //  不使用缓存查询，得到最新值
                                                        return uda.findById({
                                                                name: "user",
                                                                projection: {
                                                                    name: 1,
                                                                    age: 1
                                                                },
                                                                id
                                                            })
                                                            .then(
                                                                (result) => {
                                                                    expect(result).to.be.an("object");
                                                                    expect(result.name).to.eql("wangyuef");
                                                                    expect(result.age).to.eql(26);
                                                                    //  此时再用缓存查询，可以得到最新值
                                                                    return uda.findById({
                                                                            name: "user",
                                                                            projection: {
                                                                                name: 1,
                                                                                age: 1
                                                                            },
                                                                            useCache: true,
                                                                            id
                                                                        })
                                                                        .then(
                                                                            (result) => {
                                                                                expect(result).to.be.an("object");
                                                                                expect(result.name).to.eql("wangyuef");
                                                                                expect(result.age).to.eql(25);
                                                                            });
                                                                }
                                                            );
                                                    }
                                                );
                                        }
                                    );
                            }
                        );
                }
            )
    });

    it("[tre0.4]mtStorage，测试$lte算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        age: {
                                            $lte: 25
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 999
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        age: {
                                                            $lte: 25
                                                        }
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    age: {
                                                                        $lte: 25
                                                                    }
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.5]mtStorage，测试$lt算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        age: {
                                            $lt: 30
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 999
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        age: {
                                                            $lt: 30
                                                        }
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    age: {
                                                                        $lt: 30
                                                                    }
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.6]mtStorage，测试$gt算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        age: {
                                            $gt: 10
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 0
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        age: {
                                                            $gt: 10
                                                        }
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    age: {
                                                                        $gt: 10
                                                                    }
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.7]mtStorage，测试$gte算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        age: {
                                            $gte: 25
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 0
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        age: {
                                                            $gte: 25
                                                        }
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    age: {
                                                                        $gte: 25
                                                                    }
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.8]mtStorage，测试$ne算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        age: {
                                            $ne: 1
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 1
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        age: {
                                                            $ne: 1
                                                        }
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    age: {
                                                                        $ne: 1
                                                                    }
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.9]mtStorage，测试$nin算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 20
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        age: {
                                            $nin: [1, 25]
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 1
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        age: {
                                                            $nin: [1, 25]
                                                        }
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    age: {
                                                                        $nin: [1, 25]
                                                                    }
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.10]mtStorage，测试$between算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        age: {
                                            $between: {
                                                $left: {
                                                    $closed: true,
                                                    $value: 10
                                                },
                                                $right: {
                                                    $closed: true,
                                                    $value: 30
                                                }
                                            }
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 999
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        age: {
                                                            $between: {
                                                                $left: {
                                                                    $closed: true,
                                                                    $value: 10
                                                                },
                                                                $right: {
                                                                    $closed: true,
                                                                    $value: 30
                                                                }
                                                            }
                                                        }
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    age: {
                                                                        $between: {
                                                                            $left: {
                                                                                $closed: true,
                                                                                $value: 10
                                                                            },
                                                                            $right: {
                                                                                $closed: true,
                                                                                $value: 30
                                                                            }
                                                                        }
                                                                    }
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.11]mtStorage，测试$exists算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        age: {
                                            $exists: true
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: null
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        age: {
                                                            $exists: true
                                                        }
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    age: {
                                                                        $exists: true
                                                                    }
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.12]mtStorage，测试$or算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        $or: [
                                            {
                                                age: {
                                                    $lte: 30
                                                }
                                            },
                                            {
                                                name: {
                                                    $exists: false
                                                }
                                            }
                                        ]
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 999
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        $or: [
                                                            {
                                                                age: {
                                                                    $lte: 30
                                                                }
                                                            },
                                                            {
                                                                name: {
                                                                    $exists: false
                                                                }
                                                            }
                                                        ]
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    $or: [
                                                                        {
                                                                            age: {
                                                                                $lte: 30
                                                                            }
                                                                        },
                                                                        {
                                                                            name: {
                                                                                $exists: false
                                                                            }
                                                                        }
                                                                    ]
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.13]mtStorage，测试$and算子", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        $and: [
                                            {
                                                age: {
                                                    $lte: 30
                                                }
                                            },
                                            {
                                                name: {
                                                    $exists: true
                                                }
                                            }
                                        ]
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 999
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        $and: [
                                                            {
                                                                age: {
                                                                    $lte: 30
                                                                }
                                                            },
                                                            {
                                                                name: {
                                                                    $exists: true
                                                                }
                                                            }
                                                        ]
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    $and: [
                                                                        {
                                                                            age: {
                                                                                $lte: 30
                                                                            }
                                                                        },
                                                                        {
                                                                            name: {
                                                                                $exists: true
                                                                            }
                                                                        }
                                                                    ]
                                                                },
                                                                indexFrom: 0,
                                                                count: 10
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                                )
                                                                .then(
                                                                    ()=> {
                                                                        //  todo    这块需要增加逻辑，dummy主动通知id的entity过期
                                                                        // return uda.find({
                                                                        //     name: "user",
                                                                        //     query: {
                                                                        //         age: {
                                                                        //             $lte: 30
                                                                        //         }
                                                                        //     },
                                                                        //     indexFrom: 0,
                                                                        //     count: 10,
                                                                        //     useCache: true
                                                                        // }).then(
                                                                        //     (users)=>expect(users.length).to.equal(0)
                                                                        // )
                                                                    }
                                                                )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre0.14]测试缓存过期", () => {
        uda.constants.storageInterval = 3000;
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "user",
                                    query: {
                                        $and: [
                                            {
                                                age: {
                                                    $lte: 30
                                                }
                                            },
                                            {
                                                name: {
                                                    $exists: true
                                                }
                                            }
                                        ]
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (users)=> {
                                        expect(users.length).to.equal(1);
                                        return uda.updateOneById({
                                            name: "user",
                                            data: {
                                                age: 999
                                            },
                                            id: users[0].id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                    name: "user",
                                                    query: {
                                                        $and: [
                                                            {
                                                                age: {
                                                                    $lte: 30
                                                                }
                                                            },
                                                            {
                                                                name: {
                                                                    $exists: true
                                                                }
                                                            }
                                                        ]
                                                    },
                                                    indexFrom: 0,
                                                    count: 10,
                                                    useCache: true
                                                }).then(
                                                    (users)=> {
                                                        expect(users.length).to.equal(1)
                                                    })
                                                    .then(
                                                        ()=>new Promise(
                                                            (resolve)=> {
                                                                setTimeout(()=>resolve(), 3000)
                                                            }
                                                        )
                                                    )
                                                    .then(
                                                        ()=> {
                                                            return uda.find({
                                                                name: "user",
                                                                query: {
                                                                    $and: [
                                                                        {
                                                                            age: {
                                                                                $lte: 30
                                                                            }
                                                                        },
                                                                        {
                                                                            name: {
                                                                                $exists: true
                                                                            }
                                                                        }
                                                                    ]
                                                                },
                                                                indexFrom: 0,
                                                                count: 10,
                                                                useCache: true
                                                            }).then(
                                                                (users)=>expect(users.length).to.equal(0)
                                                            )
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    it("[tre3.0]mtStorage，测试不影响subQuery查询（在mtStorage中直接滤过，进行远端查询）", () => {
        return uda.remove({
            name: "user"
        }).then(
            ()=> {
                return uda.insert({
                        name: "user",
                        data: {
                            name: "wangyuef",
                            age: 25
                        }
                    })
                    .then(
                        (user)=>uda.insert({
                            name: "account",
                            data: {
                                ownerId: user.id,
                                deposit: 1111
                            }
                        })
                    )
                    .then(
                        (row) => {
                            return uda.find({
                                    name: "account",
                                    query: {
                                        owner: {
                                            id: row.ownerId
                                        }
                                    },
                                    indexFrom: 0,
                                    count: 10,
                                    useCache: true
                                })
                                .then(
                                    (account)=> {
                                        expect(account.length).to.equal(1);
                                        console.log("account: ", account);
                                        return uda.updateOneById({
                                            name: "account",
                                            data: {
                                                deposit: 222
                                            },
                                            id: account[0]._id
                                        }).then(
                                            ()=> {
                                                return uda.find({
                                                        name: "account",
                                                        query: {
                                                            owner: {
                                                                id: row.ownerId
                                                            },
                                                            deposit: 222
                                                        },
                                                        indexFrom: 0,
                                                        count: 10,
                                                        useCache: true
                                                    })
                                                    .then(
                                                        (account)=> {
                                                            expect(account.length).to.equal(1);
                                                        }
                                                    )
                                            }
                                        )
                                    }
                                )
                        }
                    )
            }
        )
    });

    after(() => {
        server.kill("SIGTERM");
        // server.on("exit", done);
    });
});
