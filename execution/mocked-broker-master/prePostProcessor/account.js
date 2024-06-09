const portfolioData = require("../data/portfolio").portfolioData
const constants = require("../data/constants")

exports.enquire = (req, res) => {
  const accountId = req.params.accountId;
  if (!(accountId in portfolioData)) {
    res.status(404).send({
      "data": {},
      "status": constants.apiReturnStatus.FAILURE
    })
  }
  let portfolio = portfolioData[accountId]
  let holdingDetails = portfolio.holdings.map((x) => {
    return {
      'exchange': x.exchange,
      'symbol': x.symbol,
      'averagePrice': x.averagePrice,
      'quantity': x.quantity,
      'currencyCode': x.currencyCode
    }
  })
  let cashHoldings = Object.keys(portfolio.cash).map((currencyCode) => {
    return {
      'currencyCode': currencyCode,
      'marketValue': portfolio.cash[currencyCode]
    }
  })
  res.status(200).send({
    "data": {
      "holdingDetails": holdingDetails,
      "cashHoldings": cashHoldings,
    },
    "status": constants.apiReturnStatus.SUCCESS
  })
}

exports.enquireAccounts = (req, res) => {
  let accountIdGreaterThanOrEqualsTo = req.body.accountIdGreaterThanOrEqualsTo
  let accountIdLessThanOrEqualsTo = req.body.accountIdLessThanOrEqualsTo
  let accountIds = Object.keys(portfolioData).map(id => Number(id)).filter(
    id =>
      (accountIdGreaterThanOrEqualsTo != null ? id >= Number(accountIdGreaterThanOrEqualsTo) : true) 
      && (accountIdLessThanOrEqualsTo != null ? id <= Number(accountIdLessThanOrEqualsTo) : true)
    ).map(id => String(id))
  return res.status(200).send(accountIds)
}

exports.deleteAccounts = (req, res) => {
  let accountIdGreaterThanOrEqualsTo = req.body.accountIdGreaterThanOrEqualsTo
  let accountIdLessThanOrEqualsTo = req.body.accountIdLessThanOrEqualsTo
  let accountIds = Object.keys(portfolioData).map(id => Number(id)).filter(
    id =>
      (accountIdGreaterThanOrEqualsTo != null ? id >= Number(accountIdGreaterThanOrEqualsTo) : true) 
      && (accountIdLessThanOrEqualsTo != null ? id <= Number(accountIdLessThanOrEqualsTo) : true)
    ).map(id => String(id))
  let portfolioDataToBeDeleted = accountIds.map(id => portfolioData[id])
  accountIds.forEach(id => delete portfolioData[id])
  return res.status(200).send({"deletedPortfolioData": portfolioDataToBeDeleted})
}

exports.create = (req, res) => {
  let accountId = req.body.accountId
  if (accountId in portfolioData) {
    res.status(400).send("Account " + accountId + " alreadly exist")
  } else {
    let portfolio = {
      "cash": {},
      "holdings": []
    }
    if (req.body.hasOwnProperty('cash')) {
      portfolio.cash = req.body.cash
    }
    if (req.body.hasOwnProperty('holdings')) {
      portfolio.holdings = req.body.holdings
    }
    portfolioData[accountId] = portfolio
    res.status(200).send("Success!")
  }
}

exports.modify = (req, res) => {
  let accountId = req.params.accountId
  if (!(accountId in portfolioData)) {
    res.status(404).send({
      "data": {},
      "status": constants.apiReturnStatus.FAILURE
    })
  }
  let cashHoldings = portfolioData[accountId]['cash']
  Object.keys(req.body.cash).forEach((currencyCode) => {
    if (currencyCode in cashHoldings) {
      cashHoldings[currencyCode] += Number(req.body.cash[currencyCode])
    } else {
      cashHoldings[currencyCode] = Number(req.body.cash[currencyCode])
    }
  })
  portfolioData[accountId]['cash'] = cashHoldings
  exports.enquire(req, res)
}

exports.reset = (req, res) => {
  let accountId = req.params.accountId
  if (!(accountId in portfolioData)) {
    res.status(404).send({
      "data": {},
      "status": constants.apiReturnStatus.FAILURE
    })
  }
  let portfolio = {
    "cash": {},
    "holdings": []
  }
  if (req.body.hasOwnProperty('cash')) {
    portfolio.cash = req.body.cash
  }
  if (req.body.hasOwnProperty('holdings')) {
    portfolio.holdings = req.body.holdings
  }
  portfolioData[accountId] = portfolio
  res.status(200).send(portfolio)
}