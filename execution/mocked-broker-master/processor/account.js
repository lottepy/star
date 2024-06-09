var portfolioData = require('../data/portfolio').portfolioData
const orderSideEnum = require('../data/constants').orderSide

exports.adjustPortfolioForExecution = (order, priceFilled, quantityFilled) => {
  let accountId = order.accountId, symbol = order.symbol, side = order.side, exchange = order.exchange, currencyCode = order.currencyCode
  let portfolio = JSON.parse(JSON.stringify(portfolioData[accountId]))
  let cash = portfolio.cash[currencyCode]
  if (side == orderSideEnum.BUY) {
    cash -= priceFilled * quantityFilled
  } else {
    cash += priceFilled * quantityFilled
  }
  portfolio['cash'][currencyCode] = cash

  let holdings = portfolio['holdings']
  let symbolInHoldings = holdings.filter((x) => x.symbol == symbol)
  if (symbolInHoldings.length == 1) {
    symbolInHoldings = symbolInHoldings[0]
    holdings = holdings.filter((x) => x.symbol != symbol)
    if (side == orderSideEnum.BUY) {
      symbolInHoldings['averagePrice'] = (symbolInHoldings['averagePrice'] * symbolInHoldings['quantity'] + priceFilled * quantityFilled) / (symbolInHoldings['quantity'] + quantityFilled)
      symbolInHoldings['quantity'] += quantityFilled
    } else {
      symbolInHoldings['averagePrice'] = (symbolInHoldings['averagePrice'] * symbolInHoldings['quantity'] - priceFilled * quantityFilled) / (symbolInHoldings['quantity'] - quantityFilled)
      symbolInHoldings['quantity'] -= quantityFilled
    }
    if (symbolInHoldings['quantity'] != 0) {
      holdings.push(symbolInHoldings)
    }
  } else {
    holdings.push({
      'exchange': exchange,
      'symbol': symbol,
      'averagePrice': priceFilled,
      'quantity': quantityFilled
    })
  }
  portfolio['holdings'] = holdings
  portfolioData[accountId] = portfolio
}