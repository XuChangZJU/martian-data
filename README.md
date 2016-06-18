# martian-data
（NodeJS）使用统一接口访问MySQL、MongoDB及远端数据库

本模块为杭州码天科技有限公司开发，尚未完全完成

本模块的目标是为持久层数据访问提供统一的上层接口封装，业务层在大多数时候无需关心数据是存储在哪个持久层中，目前支持的持久层包括关系数据库MySQL及非关系数据库MongoDB，我们认为这两种数据库结合使用可以满足绝大部分的业务性需求。本模块也将定义规范地通过HTTP访问远端RESTFUL的服务器，将之作为一个数据持久层。例如可以将用户表（user）看作是存储在远端身份验证服务器上的数据，而业务服务器如同访问本地数据一样进行访问。

## 定义数据源
第一步需要定义你的数据源，即数据库访问对象

```
const dataSources = {
    mysql : {
        type: "mysql",
        url: "mysql://root@localhost:3306/test",
        settings: {
            disableUpdateAt: false,
            disableCreateAt: false,
            disableDeleteAt: false    // 这三个值决定了在表中是否有相关动作的时间列。如果disableDeleteAt被设置为true，则删除是真的从数据库中删除
        }
    },
    mongodb: {
        type: "mongodb",
        url: "mongodb://localhost:27017/test"
    }
};
```
数据源对象中的每个属性代表一个数据源，每个数据源的type定义了其类型，目前支持两种类型mysql与mongodb，数据源的url定义了其访问路径。不同的数据源有一些配置参数，目前支持的配置参数中，比较重要的是mysql中的三个参数：

* __disableCreateAt__ 置为false(默认)则会在数据库创建表时默认创建一列 \_\_createAt\_\_，存储其创建时间；置为true则不会创建此列
* __disableUpdateAt__ 置为false(默认)则会在数据库创建表时默认创建一列 \_\_updateAt\_\_，存储每次的更新时间；置为true则不会创建此列
* __disableDeleteAt__ 置为false(默认)则会在数据库创建表时默认创建一列 \_\_deleteAt\_\_，当行被删除时记录时间，当查询时不会返回该行。置为true则不会创建此列，且删除数据时会直接删除

定义了数据源后，可以使用martian-data来连接数据库源

```
const martianData = require("martian-data");

martianData.connect(dataSources)
  .then(
    () => {
      console.log("success");
    },
    (err) => {
      console.log(err);
    }
  );
```
注意martian-data的所有异步访问接口，返回的都是Promise对象。

## 定义数据模式
