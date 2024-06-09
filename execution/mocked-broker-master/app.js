const express = require("express");
const app = express();
const prePostProcessor = require('./prePostProcessor')
const utils = require('./utils')
const bodyParser = require('body-parser')

// parse application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: false }))
// parse application/json
app.use(bodyParser.json())

// log all requests
app.all('*', (req, res, next) => { utils.logRequest(req); next() })

// ORDER - USER
app.post('/api/trade/order', prePostProcessor.prePostOrderUser.create)
app.get('/api/trade/order/:orderId', prePostProcessor.prePostOrderUser.enquire)
app.put('/api/trade/order/:orderId', prePostProcessor.prePostOrderUser.modify)
app.delete('/api/trade/order/:orderId', prePostProcessor.prePostOrderUser.cancel)

// // ORDER - DEALER
app.post('/api/trade/dealer/partiallyFill/:orderId', prePostProcessor.prePostOrderDealer.partiallyFill)
app.post('/api/trade/dealer/fullyFill/:orderId', prePostProcessor.prePostOrderDealer.fullyFill)

// // ACCOUNT
app.post('/api/account', prePostProcessor.prePostAccount.create)
app.post('/api/account/transfer/:accountId', prePostProcessor.prePostAccount.modify)
app.put('/api/account/reset/:accountId', prePostProcessor.prePostAccount.reset)
app.get('/api/account/:accountId', prePostProcessor.prePostAccount.enquire)

app.get('/api/accounts', prePostProcessor.prePostAccount.enquireAccounts)
app.delete('/api/accounts', prePostProcessor.prePostAccount.deleteAccounts)

// OPERATION - DATA
app.post('/api/saveDB', prePostProcessor.prePostOperationData.saveDB)
app.post('/api/readDB', prePostProcessor.prePostOperationData.readDB)

// OPERATION - EXECUTION POLICY
app.put('/api/policy/:policyName', prePostProcessor.prePostOperationExecutionPolicy.modifyDefault)
app.put('/api/policy/:symbol/:policyName', prePostProcessor.prePostOperationExecutionPolicy.modifySymbol)
app.delete('/api/policy/:symbol', prePostProcessor.prePostOperationExecutionPolicy.restoreDefaultSymbol)

app.listen(1833, '0.0.0.0')