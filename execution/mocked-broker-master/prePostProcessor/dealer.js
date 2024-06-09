const dealerProcessor = require("../processor/dealer")
const floatedOrders = require("../data/floatedOrders").floatedOrders

exports.partiallyFill = (req, res) => {
  let orderId = req.params.orderId, price = req.query.price || -1, quantity = req.query.quantity || -1
  if (!floatedOrders.hasOwnProperty(orderId)) {
    res.status(404).send(`Order with orderId [${orderId}] not found.`)
  } else {
    let order = floatedOrders[orderId]
    order = dealerProcessor.partiallyFilled(order, quantity, price)
    floatedOrders[orderId] = order
    res.status(200).send("Success!")
  }
}

exports.fullyFill = (req, res) => {
  let orderId = req.params.orderId, price = req.query.price || -1
  if (!floatedOrders.hasOwnProperty(orderId)) {
    res.status(404).send(`Order with orderId [${orderId}] not found.`)
  } else {
    let order = floatedOrders[orderId]
    order = dealerProcessor.partiallyFilled(order, order.quantity - (order.filledQuantity || 0), price)
    floatedOrders[orderId] = order
    res.status(200).send("Success!")
  }
}