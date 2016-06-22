/**
 * Created by Administrator on 2016/6/22.
 */
module.exports = {
    user: {
        source: "mysql",
        attributes: {
            name: {
                type: "string",
                required: true
            },
            age: {
                type: "int"
            }
        }
    },
    account: {
        source: "mongodb",
        attributes: {
            owner: {
                type: "ref",
                ref: "user",
                required: true
            },
            deposit: {
                type: "number",
                required: true
            }
        }
    }
};

