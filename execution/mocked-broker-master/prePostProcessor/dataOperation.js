var floatedOrders = require('../data/floatedOrders').floatedOrders
var portfolioData = require("../data/portfolio").portfolioData

exports.saveDB = (req, res) => {
    var dataSaver = {};
    dataSaver['portfolioData'] = portfolioData;
    dataSaver['floatedOrders'] = floatedOrders;
    var dataSaverStr = JSON.stringify(dataSaver);
    console.log(dataSaverStr);

    var fs = require("fs");
    console.log("准备写入文件");
    var filename = './data/db.json';
    fs.writeFile(filename, dataSaverStr, function (err) {
        if (err) {
            return console.error(err);
        }
        console.log("数据写入成功！");
    });

    res.status(200).send({
        'status': "SUCCESS"
    })
}

exports.readDB = (req, res) => {
  var fs = require("fs");
  console.log("准备读取文件");
  var filename = './data/db.json';
  fs.readFile(filename, function (err, data) {
      if (err) {
          return console.error(err);
      }
      console.log("异步读取文件数据: " + data.toString());

      var dataLoader = JSON.parse(data);
      console.log(dataLoader);
      Object.assign(portfolioData, dataLoader['portfolioData']);
      Object.assign(floatedOrders, dataLoader['floatedOrders']);
  });

  res.status(200).send({
      'status': "SUCCESS"
  })
}