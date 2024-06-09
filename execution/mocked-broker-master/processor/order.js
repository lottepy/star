var floatedOrders = require('../data/floatedOrders').floatedOrders
const orderMetaProcessors = require("./orderMetaProcessors").statusAndExecutionProcessor
var getProcedure = require('./procedures').getProcedure
const orderStatusEnum = require("../data/constants").orderStatus

exports.create = (orderInput) => {
  let order = JSON.parse(JSON.stringify(orderInput))
  floatedOrders[order.orderId] = order
  
  let idx = 0
  let runProcedure = (idx) => {
    order = floatedOrders[order.orderId]
    floatedOrders[order.orderId] = getProcedure(orderInput)[idx](order)
  }
  runProcedure(idx++)
  let interval = setInterval(() => {
    if (idx >= getProcedure(orderInput).length) {
      clearInterval(interval)
    } else {
      runProcedure(idx++)
    }
  }, 3000)
}

// 由于处理步骤很简单，因此就不调用 procedure 了
exports.modify = (orderInput, price) => {
  let order = JSON.parse(JSON.stringify(orderInput))
  order.price = price
  floatedOrders[order.orderId] = order
}

// 由于处理步骤很简单，因此就不调用 procedure 了
exports.cancel = (order) => {
  floatedOrders[order.orderId] = orderMetaProcessors.cancelled(order)
}