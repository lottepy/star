const utils = require('../utils')
var floatedOrders = require('../data/floatedOrders').floatedOrders
const constants = require('../data/constants')
const orderProcessor = require('../processor').orderProcessor
const verifier = require("./verifier")

exports.create = (req, res) => {
  let orderId = utils.getRandomOrderId();
  let order = {'status': constants.orderStatus.PENDING};
  order['orderId'] = orderId;
  order['accountId'] = req.body.accountId;
  order['channel'] = req.body.channel;
  order['exchange'] = req.body.exchange;
  order['orderType'] = req.body.orderType;
  order['price'] = Number(req.body.price);
  order['filledPrice'] = 0;
  order['quantity'] = order['outstandingQuantity'] = Number(req.body.quantity);
  order['filledQuantity'] = 0;
  order['side'] = req.body.side;
  order['subChannel'] = req.body.subChannel;
  order['symbol'] = req.body.symbol;
  order['tradeSolicitation'] = req.body.tradeSolicitation;
  order['createdTime'] = utils.getDateString();
  order['executions'] = []
  order['currencyCode'] = req.body.currencyCode

  try {
    // verifier.verifyCreation(order)
    orderProcessor.create(order);
    console.log(utils.LOG_PREFIX, " : ORDER PLACED: ", order)
    res.status(200).send({
      "data": {
        "orderId": orderId
      },
      "status": constants.apiReturnStatus.SUCCESS
    })
  } catch (e) {
    res.status(400).send({
      "data": {
        "orderId": orderId
      },
      "status": constants.apiReturnStatus.FAILURE,
      "errors": [{
        "code": -1,
        "category": "VALIDATION_ERROR",
        "message": e
      }]
    })
  }
}

exports.enquire = (req, res) => {
  let orderId = req.params.orderId;
  if (!(orderId in floatedOrders)) {
      res.status(404).send({
          "data": {},
          "status": constants.apiReturnStatus.FAILURE,
          "errors": [{
            "code": -1,
            "category": "VALIDATION_ERROR",
            "message": "order not found"
          }]
      })
      return
  }

  let floatedOrder = floatedOrders[orderId]
  let returnOrder = {
      'orderId': orderId,
      'exchange': floatedOrder.exchange || 'XXX',
      'symbol': floatedOrder.symbol,
      'currencyCode': floatedOrder.order,
      'underlyingSymbol': null,
      'timeInForce': 'DAY',
      'quantity': floatedOrder.quantity,
      'filledQuantity': floatedOrder.filledQuantity || 0,
      'filledPrice': floatedOrder.filledPrice || 0,
      'outstandingQuantity': floatedOrder.quantity - (floatedOrder.filledQuantity || 0),
      'reducedQuantity': floatedOrder.reducedQuantity || 0,
      'cancelledQuantity': (floatedOrder.status == constants.orderStatus.CANCELLED
          || floatedOrder.status == constants.orderStatus.PARTIAL_FILLED_CANCELLED)
          ? floatedOrder.quantity - (floatedOrder.filledQuantity || 0) : 0,
      'side': floatedOrder.side,
      'price': floatedOrder.price,
      'currencyCode': 'XXX',
      'orderType': floatedOrder.orderType,
      'qualifier': null,
      'status': floatedOrder.status,
      'createdTime': floatedOrder.createdTime,
      'touchPrice': null,
      'filledDetail': {
          'executions': floatedOrder.executions
      }
  };

  res.status(200).send({
    'data': {
      'order': returnOrder,
    },
    'status': constants.apiReturnStatus.SUCCESS
  })
}

exports.modify = (req, res) => {
  const orderId = req.params.orderId;
  const price = req.query.price;
  if (!(orderId in floatedOrders)) {
    res.status(404).send({
        "status": constants.apiReturnStatus.FAILURE,
        "errors": [{
          "code": -1,
          "category": "VALIDATION_ERROR",
          "message": "order not found"
        }]
    })
    return
  }
  try {
    let order = floatedOrders[orderId]
    verifier.verifyModification(order)
    orderProcessor.modify(order, price);
    res.status(200).send({
      'status': "SUCCESS"
    }) 
  } catch (e) {
    res.status(400).send({
      "data": {
        "orderId": orderId
      },
      "status": constants.apiReturnStatus.FAILURE,
      "errors": [{
        "code": -1,
        "category": "VALIDATION_ERROR",
        "message": e
      }]
    })
  }
}

exports.cancel = (req, res) => {
  const orderId = req.params.orderId;
  if (!(orderId in floatedOrders)) {
    res.status(404).send({
        "status": constants.apiReturnStatus.FAILURE,
        "errors": [{
          "code": -1,
          "category": "VALIDATION_ERROR",
          "message": "order not found"
        }]
    })
    return
  }
  // try {
    let order = floatedOrders[orderId]
    verifier.verifyCancelation(order)
    orderProcessor.cancel(order);
    res.status(200).send({
      'status': "SUCCESS"
    }) 
  // } catch (e) {
  //   res.status(400).send({
  //     "data": {
  //       "orderId": orderId
  //     },
  //     "status": constants.apiReturnStatus.FAILURE,
  //     "errors": [{
  //       "code": -1,
  //       "category": "VALIDATION_ERROR",
  //       "message": e
  //     }]
  //   })
  // }
}