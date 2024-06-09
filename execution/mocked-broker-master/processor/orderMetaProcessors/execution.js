const orderStatusEnum = require('../../data/constants').orderStatus
const orderTypeEnum = require('../../data/constants').orderType
const orderSideEnum = require('../../data/constants').orderSide
const accountProcessor = require('../account')
const utils = require('../../utils')

function logExecution(order, quantityFilledForThisRound, priceFilledForThisTime) {
  console.log(utils.LOG_PREFIX, ': EXECUTION ', order.accountId, order.orderId + ': current filledPrice: ' + order.filledPrice + ', current filledQuantity: ' + order.filledQuantity + ', quantityFilledForThisRound: ' + quantityFilledForThisRound, ', priceFilledForThisTime: ' + priceFilledForThisTime)
}

exports.pendingToUnfilled = (order) => {
  if (order.status == orderStatusEnum.PENDING) {
    order.status = orderStatusEnum.UNFILLED
    return order
  } else {
    return null
  }
}

/**
 * 
 * @param {Order} orderInput 
 * @param {Number} quantityFilledSpecified let unfilled or <= 0,
 * we will generate the quantity automatically.
 * If specified a number > 0, we will execute as specified
 */
exports.partiallyFilled = (orderInput, quantityFilledSpecified=-1, priceFilledSpecified=-1) => {
  let order = JSON.parse(JSON.stringify(orderInput)); // deep copy
  if (![orderStatusEnum.UNFILLED, orderStatusEnum.PARTIAL_FILLED].includes(order.status)) {
    return orderInput
  }
  if (quantityFilledSpecified > orderInput.outstandingQuantity) {
    return orderInput
  }

  let quantityFilled = quantityFilledSpecified > 0
                          ? quantityFilledSpecified
                          : order.outstandingQuantity == 1
                              ? 1
                              : Math.floor(order.outstandingQuantity / 2);
  let priceFilled = priceFilledSpecified > 0
                      ? priceFilledSpecified
                      : order.orderType == orderTypeEnum.LIMIT
                          ? order.price
                          // else: Market Order. for BUY Market Order, price moves up a bit, Sell, down.
                          : order.side == orderSideEnum.BUY
                              ? Number(order.price) + Number(utils.getRandomNumber(0, order['price'] / 500))
                              : Number(order.price) + Number(utils.getRandomNumber(-order['price'] / 500, 0))
  order.outstandingQuantity -= quantityFilled;
  order.status = order.outstandingQuantity == 0
                    ? orderStatusEnum.FILLED
                    : orderStatusEnum.PARTIAL_FILLED;
  order.filledPrice = (order.filledPrice * order.filledQuantity + quantityFilled * priceFilled) / (order.filledQuantity + quantityFilled)
  order.filledQuantity += quantityFilled;
  let execution = {
      'executionId': utils.getRandomExecutionId(),
      'filledQuantity': quantityFilled,
      'filledPrice': priceFilled,
      'filledTime': utils.getDateString()
  }
  logExecution(orderInput, quantityFilled, priceFilled)
  order.executions.push(execution);
  accountProcessor.adjustPortfolioForExecution(order, priceFilled, quantityFilled)
  return order
}

exports.fullyFilled = (orderInput) => {
  return exports.partiallyFilled(orderInput, orderInput.outstandingQuantity)
}

exports.cancelled = (orderInput) => {
  let order = JSON.parse(JSON.stringify(orderInput))
  if (order.status == orderStatusEnum.PARTIAL_FILLED) {
    order.status = orderStatusEnum.PARTIAL_FILLED_CANCELLED
  } else {
    order.status = orderStatusEnum.CANCELLED
  }
  return order
}