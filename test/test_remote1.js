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
                                                                                    resolve(server);;
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

describe("test remote 1", function() {
    this.timeout(5000);
    let server;
    before((done) => {
        init(schemaRemote1)
            .then((serv)=>{
                server = serv;
                done();
            }, done)
            .catch(done);
    });

    it("[tre0.0]", (done) => {
        // 尝试插入数据，再查询
        uda.insert("user", {
                name: "xc",
                age: 33
            })
            .then(
                (row) => {
                    let id = row.hasOwnProperty("id")? row.id : row._id;

                    uda.findById("user", {
                            name: 1,
                            age: 1
                        }, id)
                        .then(
                            (result) => {
                                expect(result).to.be.an("object");
                                expect(result.name).to.eql("xc");
                                expect(result.age).to.eql(33);
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
    });

    it("[tre0.1]", (done) => {
        // 尝试插入数据，再查询
        uda.insert("user", {
                name: "xc",
                age: 33
            })
            .then(
                (row) => {

                    let account = {
                        owner: row,
                        deposit: 10000
                    };
                    uda.insert("account", account)
                        .then(
                            (row2) => {
                                let id = row2.hasOwnProperty("id")? row2.id : row2._id;
                                uda.findById("account", {
                                        owner: {
                                            name: 1,
                                            age: 1
                                        },
                                        deposit: 1
                                    }, id)
                                    .then(
                                        (result) => {
                                            expect(result).to.be.an("object");
                                            expect(result.deposit).to.eql(10000);
                                            expect(result.owner).to.be.an("object");
                                            expect(result.owner.name).to.eql("xc");
                                            expect(result.owner.age).to.eql(33);
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
            )
    });

    it("[tre0.2]", (done) => {
        // 尝试增删改查
        uda.insert("user", {
                name: "xc",
                age: 33
            })
            .then(
                (row) => {
                    let id = row.hasOwnProperty("id")? row.id : row._id;

                    uda.findById("user", {
                            name: 1,
                            age: 1
                        }, id)
                        .then(
                            (result) => {
                                expect(result).to.be.an("object");
                                expect(result.name).to.eql("xc");
                                expect(result.age).to.eql(33);

                                uda.updateOneById("user", {
                                        $set: {
                                            age: 34
                                        }
                                    },
                                    id)
                                    .then(
                                        () => {
                                            uda.findById("user", {
                                                    name: 1,
                                                    age: 1
                                                }, id)
                                                .then(
                                                    (result) => {
                                                        expect(result).to.be.an("object");
                                                        expect(result.name).to.eql("xc");
                                                        expect(result.age).to.eql(34);

                                                        uda.removeOneById("user", id)
                                                            .then(
                                                                () => {
                                                                    uda.findById("user", {
                                                                            name: 1,
                                                                            age: 1
                                                                        }, id)
                                                                        .then(
                                                                            (result) => {
                                                                                assert(result === null);

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
            )
    });

    after((done) => {
        server.kill("SIGTERM");
        server.on("exit", done);
    });
});


describe("test remote 2", function() {
    this.timeout(5000000);
    let server;
    before((done) => {
        init(schemaRemote2)
            .then((serv)=>{
                server = serv;
                done();
            }, done)
            .catch(done);
    });



    it("[tre1.0]", (done) => {
        let now = new Date();
        // 尝试插入数据，再进行连接查询
        uda.insert("user", {
                name: "xc",
                age: 33
            })
            .then(
                (row) => {
                    let account = {
                        owner: row,
                        deposit: 10000
                    };
                    uda.insert("account", account)
                        .then(
                            (row2) => {
                                uda.insert("order", {
                                    account: row2,
                                    time: now
                                })
                                    .then(
                                        (row3) => {
                                            let projection =  {
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
                                            uda.find("order", projection, query, undefined, 0, 10)
                                                .then(
                                                    (result) => {
                                                        expect(result).to.be.an("array");
                                                        expect(result).to.have.length(1);
                                                        expect(result[0].time).to.eql(now);
                                                        done();
                                                    },
                                                    (err) =>{
                                                        done(err);
                                                    }
                                                )
                                                .catch(
                                                    (err) => {
                                                        done(err);
                                                    }
                                                )
                                        },
                                        (err) => {
                                            done(err);
                                        }
                                    )
                                    .catch(
                                        (err) => {
                                            done(err);
                                        }
                                    )
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
    });

    it("[tre0.2]", (done) => {
        // 尝试增删改查
        uda.insert("user", {
                name: "xc",
                age: 33
            })
            .then(
                (row) => {
                    let id = row.hasOwnProperty("id")? row.id : row._id;

                    uda.findById("user", {
                            name: 1,
                            age: 1
                        }, id)
                        .then(
                            (result) => {
                                expect(result).to.be.an("object");
                                expect(result.name).to.eql("xc");
                                expect(result.age).to.eql(33);

                                uda.updateOneById("user", {
                                        $set: {
                                            age: 34
                                        }
                                    },
                                    id)
                                    .then(
                                        () => {
                                            uda.findById("user", {
                                                    name: 1,
                                                    age: 1
                                                }, id)
                                                .then(
                                                    (result) => {
                                                        expect(result).to.be.an("object");
                                                        expect(result.name).to.eql("xc");
                                                        expect(result.age).to.eql(34);

                                                        uda.removeOneById("user", id)
                                                            .then(
                                                                () => {
                                                                    uda.findById("user", {
                                                                            name: 1,
                                                                            age: 1
                                                                        }, id)
                                                                        .then(
                                                                            (result) => {
                                                                                assert(result === null);

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
            )
    });

    after((done) => {
        server.kill("SIGTERM");
        server.on("exit", done);
    });
});
