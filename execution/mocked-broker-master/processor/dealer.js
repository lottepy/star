const execution = require("./orderMetaProcessors/execution")

exports.partiallyFilled = (order, quantity, price) => {
  return execution.partiallyFilled(order, quantity, price)
}

