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

将要访问的数据对象定义成为数据模式（schema），模式中的每个属性是一个数据对象，代表持久层的一张表或者集合

```
const schema = {
    house: {
        source: "mysql",    // 对应上面的dataSource数据源
        attributes: {       // 属性
            buildAt:{
                type: "date",       // 类型
                required : true,    // 是否可以为空
                defaultValue : new Date()   // 默认值 
            },
            status: {
                type: {
                    type: "enum",
                    values: ["verifying", "offline", "free", "inRent"]
                },
                required: true,
                defaultValue: "verifying"
            },
            houseInfo: {
                type: "ref",        // 引用对象
                ref: "houseInfo",   // 指向的引用对象名
                required: true,     // 是否可以为空
                localColumnName: "houseInfoId"       // 列名
                autoIndexed: false                   //  默认在外键列上会建索引，如果指定不建，这个域要加上false
            }
        },
        indexes: {
            index1: {
                    columns: {
                        status: 1           // 索引的键值
                    },
                    options : {
                        unique: true        // 是否唯一
                    }
                }
        }
    },
    houseInfo: {
        source: "mysql",
        attributes: {
            id: {
                type: "serial",
                key: true           // 主键
            },
            area:{
                type: "number",
                required : true
            },
            floor: {
                type: "int",
                required: true
            }
        }
    }
};
```
对每个数据对象主要需要定义三个属性：

### 数据源
source指定了数据对象的源，源必须在dataSource中有相应的定义

### 属性
attribute定义了数据对象的列，对于每一列，重要的定义有：

* 类型（type)，目前支持的类型包括int/integer, number, boolean, string, text, enum, date/time, serial, object, ref 对于int和number，可以定义长度：
```
type: {
    type: "int",
    size: 8
}
```
对于enum对象，可以定义其枚举值：
```
type: {
    type: "enum",
    values: ["male", "female"]
}
```
date/time 在存储时会被转换为毫秒数

当object对象被存储在mysql中时，存储的是其JSON化的字符串。尽管mysql5.7以上支持JSON数据，但我们认为需要在上执行复杂查询的JSON数据还是应该被存放在mongodb中。

serial对象是自增列，只对mysql支持，一般用用于定义主键。每个对象只能有一个自增列作为主键，如果不显式指定，则martianData会创建默认的主键值，在mysql中这列是id，在mongodb中使用其默认的主键列_id。我们推荐不要创建显式主键。不支持复合主键。

ref属性是指向另一个对象的引用，相当于数据库的外键。所指向对象必须用ref属性中的ref属性来指定，此对象必须在schema中定义，但可以处于另外的源中。martian-data在创建对象时，对于ref对象会创建一列指向被引用对象的主键，还可以指定列名（localColumnName）和是否创建索引（autoIndexed），默认情况下用ref对象的对象名加上Id作为列名，并创建索引。

* 是否为主键（key），只对mysql支持。mongodb的主键是其默认的主键_id
* 是否不为空（required)
* 默认值（defaultValue）

### 索引
indexes定义了对象上的索引，对每个索引，需要定义：
* columns ：索引的相关列，可以定义多列上的复合索引，以及索引键值的顺序，1为升序，-1为降序
* options：索引的属性，包括unique（是否唯一）

定义了数据模式后，可以为matianData设置数据源：
```
martianData.setSchemas(schema);
```
再创建相应的数据对象：
```
martianData.createSchemas()
    .then(
        () => {
            console.log("create schemas success");
        },
        (err) => {
            console.log("create schemas fail");
        }
    );
```
