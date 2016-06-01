/**
 * Created by Administrator on 2016/6/1.
 */

var expect = require("expect.js");


const dataAccess = require("../src/dataAccess");
const dataSource = require("./def/dataSource");

describe('dataAccess', () => {
    describe("test create database", () => {
        it("test connect", (done) => {
            dataAccess.setDataSource(dataSource)
                .then(
                    () => {
                        expect(dataAccess.connections).to.be.an("object");
                        expect(dataAccess.connections).to.have.property("mysql");
                        expect(dataAccess.connections).to.have.property("mongodb");
                        expect(dataAccess.connections.mysql.settings.get("connection.pool")).to.be(true);
                        done();
                    },
                    (err) => {
                        done(err);
                    }
                );
        })
    });
});
