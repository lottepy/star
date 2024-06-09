const orderSideEnum = require("../data/constants").orderSide
const orderStatusEnum = require("../data/constants").orderStatus
const instruments = require("../data/instruments").instruments

let verifyLotSize = (order) => {
  if (order.exchange == "CN" && order.side == orderSideEnum.SELL) {
    return true
  }

  let instrumentInfo = instruments.filter( x => x.symbol == order.symbol)
  if (instrumentInfo.length == 0) {
    throw "invalid symbol or symbol not supported"
  } else {
    instrumentInfo = instrumentInfo[0]
    if (order.outstandingQuantity % instrumentInfo.lotSize == 0) {
      return true
    } else {
      throw `Received quantity [${order.outstandingQuantity}] for [${order.symbol}] is not a multiple of its lot size [${instrumentInfo.lotSize}]`
    }
  }
}

let isTerminalStatus = (status) => {
  return ([orderStatusEnum.CANCELLED, orderStatusEnum.FILLED, orderStatusEnum.PARTIAL_FILLED_CANCELLED, orderStatusEnum.PARTIAL_FILLED_REJECTED, orderStatusEnum.REJECTED].includes(status)) ? true : false
} 

exports.verifyCreation = (order) => {
  return verifyLotSize(order)
}

exports.verifyModification = (order) => {
  if (isTerminalStatus(order.status)) {
    throw `Order [${order.orderId}] is in terminal status [${order.status}]`
  }
}

exports.verifyCancelation = (order) => {
  if (isTerminalStatus(order.status)) {
    throw `Order [${order.orderId}] is in terminal status [${order.status}]`
  }
}