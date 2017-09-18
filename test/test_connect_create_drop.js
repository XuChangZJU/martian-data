/**
 * Created by Administrator on 2016/6/1.
 */
"use strict";

var expect = require("expect.js");

const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dataSource = require("./def/dataSource");
const schema = require("./def/schemas/schema");
const schema2 = require("./def/schemas/schema2");
const schema3 = require("./def/schemas/schema3");


function createInsert(uda, tableName, data) {
    return uda.createSchemas()
        .then(
            () => {
                try {
                    return uda.insert({
                            name: tableName,
                            data
                        })
                        .then(
                            (results) => {
                                console.log(results);
                                return Promise.resolve();
                            },
                            (err) => {
                                return Promise.reject(err);
                            }
                        );
                }
                catch (e) {
                    return Promise.reject(e);
                }
            },
            (err) => {
                return Promise.reject(err);
            }
        );
}

function dropCreateInsert(uda, tableName, data, needCreateBeforeInsert) {
    return uda.dropSchemas()
        .then(
            () => {
                return uda.insert({
                        name: tableName,
                        data
                    })
                    .then(
                        () => {
                            if (needCreateBeforeInsert) {
                                return Promise.reject(new Error("不应该能执行到这里"));
                            }
                            else {
                                // 如果是mongo这种不需要先创建表的，可以插入成功，但此时再创建表肯定要失败
                                return createInsert(uda, tableName, data)
                                    .then(
                                        (result) => {
                                            // 现在允许不删除表了，所以这个判断不再成立
                                            return Promise.resolve(result);
                                            // return Promise.reject(new Error("不应该能执行到这里"));
                                        },
                                        (err) => {
                                            return uda.dropSchemas()
                                                .then(
                                                    () => {
                                                        return createInsert(uda, tableName, data);
                                                    },
                                                    (err) => {
                                                        return Promise.reject(err);
                                                    }
                                                );
                                        }
                                    );
                            }
                        },
                        (err) => {
                            if (needCreateBeforeInsert) {
                                expect(err.code).to.eql("ER_NO_SUCH_TABLE");
                                return createInsert(uda, tableName, data);
                            }
                            else {
                                console.log(err);
                                return Promise.reject(new Error("不应该能执行到这里"));
                            }
                        }
                    );
            },
            (err) => {
                return Promise.reject(err);
            }
        )
}

describe('uda', () => {
    describe("test connect database", () => {
        it("[0.0]test connect", (done) => {
            uda.connect(dataSource)
                .then(
                    () => {
                        uda.disconnect()
                            .then(
                                () => {
                                    done()
                                },
                                (err) => {
                                    done(err)
                                }
                            );
                    },
                    (err) => {
                        done(err);
                    }
                );
        })
    });

    describe("test create and drop schemas", () => {
        it("[1.0]test create and drop schemas in mysql", (done) => {
            uda.connect(dataSource)
                .then(
                    () => {
                        let _schema = JSON.parse(JSON.stringify(schema));
                        uda.setSchemas(_schema)
                            .then(
                                () => {
                                    dropCreateInsert(uda, "house", {
                                        buildAt: new Date(),
                                        status: "offline"
                                    }, true)
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
                            )
                    },
                    (err) => {
                        done(err);
                    }
                )
        });

        it("[1.1]test create and drop schemas in mongodb", (done) => {
            /* 这个测试只能对一张表进行，mongodb的并发创建表和删除表貌似有多版本的问题（或者是先后顺序的问题），有时候会出些奇怪的问题
             * 但这个应该不影响实际生产环境，因为在生产环境中不会有这种行为
             *  by xc 20160505
             */
            uda.connect(dataSource)
                .then(
                    () => {
                        let _schema2 = JSON.parse(JSON.stringify(schema2));
                        uda.setSchemas(_schema2)
                            .then(
                                () => {
                                    dropCreateInsert(uda, "house", {
                                        buildAt: new Date(),
                                        status: "offline"
                                    }, false)
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
                )
        });
    });


    describe("test create schema with indexes", () => {
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

        it("[2.0]in mysql and mongodb", (done) => {
            let _schema3 = JSON.parse(JSON.stringify(schema));

            _schema3.house.indexes = {
                index1: {
                    columns: {
                        status: 1
                    },
                    options: {
                        unique: true
                    }
                }
            };
            _schema3.houseInfo.source = "mongodb";

            _schema3.houseInfo.indexes = {
                index_area: {
                    columns: {
                        area: 1
                    },
                    options: {
                        unique: true
                    }
                },
                index_area_floor: {
                    columns: {
                        area: 1,
                        floor: 1
                    }
                }
            };

            uda.setSchemas(_schema3)
                .then(
                    () => {
                        uda.dropSchemas()
                            .then(
                                () => {
                                    uda.createSchemas()
                                        .then(
                                            () => {
                                                console.log("!请去相应表上检查有无索引!");
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

        after((done) => {
            uda.disconnect()
                .then(
                    (result) => {
                        done();
                    },
                    (err) => {
                        done(err);
                    }
                );
        })
    });

    describe("test create schema with refs", function () {
        this.timeout(4000);
        before((done) => {
            uda.connect(dataSource)
                .then(done);
        });

        it("[3.0]master and slave are both in mysql", (done) => {

            let _schema3 = JSON.parse(JSON.stringify(schema3));
            uda.setSchemas(_schema3)
                .then(
                    () => {
                        uda.dropSchemas()
                            .then(
                                () => {
                                    uda.createSchemas()
                                        .then(
                                            () => {
                                                console.log("请查看house表的定义是否存在houseInfoId列");
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

        it("[3.1]master in mysql, slave in mongodb", (done) => {
            //let _schema2 = Object.assign({}, schema3);
            let _schema2 = JSON.parse(JSON.stringify(schema3));
            _schema2.houseInfo.source = "mongodb";
            uda.setSchemas(_schema2)
                .then(
                    () => {
                        uda.dropSchemas()
                            .then(
                                () => {
                                    uda.createSchemas()
                                        .then(
                                            () => {
                                                console.log("请查看house表的定义是否存在houseInfoId列");
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

        it("[3.2]master and slave are both in mysql, insert after create", (done) => {

            let _schema2 = JSON.parse(JSON.stringify(schema3));
            uda.setSchemas(_schema2)
                .then(
                    () => {
                        uda.dropSchemas()
                            .then(
                                () => {
                                    uda.createSchemas()
                                        .then(
                                            () => {
                                                let house = {
                                                    buildAt: new Date(),
                                                    status: "verifying"
                                                };
                                                let houseInfo = {
                                                    area: 55,
                                                    floor: 6
                                                };
                                                uda.insert({
                                                        name: "houseInfo",
                                                        data: houseInfo
                                                    })
                                                    .then(
                                                        (hiItem) => {
                                                            houseInfo.id = hiItem.id || hiItem._id;
                                                            house.houseInfo = houseInfo;
                                                            uda.insert({
                                                                    name: "house",
                                                                    data: house
                                                                })
                                                                .then(
                                                                    (hItem) => {
                                                                        console.log(hItem);
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
        });

        it("[3.3]master in mysql, slave in mongodb, insert after create", (done) => {

            let _schema2 = JSON.parse(JSON.stringify(schema3));
            _schema2.houseInfo.source = "mongodb";
            uda.setSchemas(_schema2)
                .then(
                    () => {
                        uda.dropSchemas()
                            .then(
                                () => {
                                    uda.createSchemas()
                                        .then(
                                            () => {
                                                let house = {
                                                    buildAt: new Date(),
                                                    status: "verifying"
                                                };
                                                let houseInfo = {
                                                    area: 55,
                                                    floor: 6
                                                };
                                                uda.insert({
                                                        name: "houseInfo",
                                                        data: houseInfo
                                                    })
                                                    .then(
                                                        (hiItem) => {
                                                            house.houseInfo = hiItem;
                                                            uda.insert({
                                                                    name: "house",
                                                                    data: house
                                                                })
                                                                .then(
                                                                    (hItem) => {
                                                                        console.log(hItem);
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
        });

        it("[3.4]master in mongodb, slave in mysql, insert after create", (done) => {
            let _schema2 = JSON.parse(JSON.stringify(schema3));

            _schema2.house.source = "mongodb";
            uda.setSchemas(_schema2)
                .then(
                    () => {
                        uda.dropSchemas()
                            .then(
                                () => {
                                    uda.createSchemas()
                                        .then(
                                            () => {
                                                let house = {
                                                    buildAt: new Date(),
                                                    status: "verifying"
                                                };
                                                let houseInfo = {
                                                    area: 55,
                                                    floor: 6
                                                };
                                                uda.insert({
                                                        name: "houseInfo",
                                                        data: houseInfo
                                                    })
                                                    .then(
                                                        (hiItem) => {
                                                            houseInfo.id = hiItem.id || hiItem._id;
                                                            house.houseInfo = houseInfo;
                                                            uda.insert({
                                                                    name: "house",
                                                                    data: house
                                                                })
                                                                .then(
                                                                    (hItem) => {
                                                                        console.log(hItem);
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
        });

        it("[3.5]master and slave are both in mysql, insert using directId after create", (done) => {
            let _schema2 = JSON.parse(JSON.stringify(schema3));

            uda.setSchemas(_schema2)
                .then(
                    () => {
                        uda.dropSchemas()
                            .then(
                                () => {
                                    uda.createSchemas()
                                        .then(
                                            () => {
                                                let house = {
                                                    buildAt: new Date(),
                                                    status: "verifying"
                                                };
                                                let houseInfo = {
                                                    area: 55,
                                                    floor: 6
                                                };
                                                uda.insert({
                                                        name: "houseInfo",
                                                        data: houseInfo
                                                    })
                                                    .then(
                                                        (hiItem) => {
                                                            houseInfo.id = hiItem.id || hiItem._id;
                                                            house.houseInfoId = houseInfo.id;
                                                            uda.insert({
                                                                    name: "house",
                                                                    data: house
                                                                })
                                                                .then(
                                                                    (hItem) => {
                                                                        console.log(hItem);
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
        });

        after((done) => {
            uda.disconnect()
                .then(done);
        });
    })
});
