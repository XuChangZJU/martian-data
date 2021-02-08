
const expect = require("expect.js");
const cloneDeep = require('lodash/cloneDeep');

const UDA = require("../src/UnifiedDataAccess");
const uda = new UDA();
const dataSource = require("./def/dataSource");
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

async function initData(uda, users, houses, houseInfos) {
    await uda.dropSchemas();
    await uda.createSchemas();
    const usersInserted = await Promise.all(
        users.map(
            async ele => {
                return await uda.insert({
                    name: 'user',
                    data: ele,
                });
            }
        )
    );

    return await Promise.all(
        houseInfos.map(
            async (ele, idx) => {
                const houseInfoInserted = await uda.insert({
                    name: 'houseInfo',
                    data: ele,
                });
                houses[idx].houseInfoId = houseInfoInserted.id;

                const houseInserted = await uda.insert({
                    name: 'house',
                    data: houses[idx],
                });

                let contract = {
                    owner: usersInserted[idx * 2],
                    renter: usersInserted[idx * 2 + 1],
                    price: 2000
                };

                const contractInserted = await uda.insert({
                    name: 'contract',
                    data: contract,
                });
                
                await uda.updateOneById({
                    name: 'house',
                    data: {
                        contractId: contractInserted.id,
                    },
                    id: houseInserted.id,
                });
            }
        )
    );
}


describe ('test cache', function() {
    this.timeout(10000);

    before(async () => {
        await uda.connect(dataSource);
        const _schema4 = cloneDeep(schema4);
        await uda.setSchemas(_schema4);
        await initData(uda, g_users, g_houses, g_houseInfos);
    });

    /**
     * 这个用例就debug跟一下吧，看看txnCache有没有起作用
     */
    it('[tcache1.0]findById从cache走', async () => {
        const txn = await uda.startTransaction('mysql');
        const result = await uda.find({
            name: 'house',
            indexFrom: 0,
            count: 10,
            txn,
        });

        console.log('请DEBUG跟一下，这次应该不走cache');
        const result2 = await uda.findById({
            name: 'house',
            id: result[0].id,
            txn,
        });


        console.log('请DEBUG跟一下，这次应该走cache');
        const result3 = await uda.findById({
            name: 'house',
            id: result[0].id,
            projection: {
                buildAt: 1,
                status: 1,
            },
            txn,
        })

        await uda.commitTransaction(txn);
    });
});