const procedureProcessor = require("../processor/procedures")

exports.modifyDefault = (req, res) => {
  let policy = req.params.policyName
  if (!procedureProcessor.procedures.hasOwnProperty(policy)) {
    res.status(400).send("Policy not supported. Availables are: " + Object.keys(procedureProcessor.procedures).join(', '))
  } else {
    procedureProcessor.setDefaultPocedure(policy)
    res.status(200).send("succeess!")
  }
}


exports.modifySymbol = (req, res) => {
  let symbol = req.params.symbol
  let policy = req.params.policyName
  if (!procedureProcessor.procedures.hasOwnProperty(policy)) {
    res.status(400).send("Policy not supported. Availables are: " + Object.keys(procedureProcessor.procedures).join(', '))
  } else {
    procedureProcessor.setSymbolPocedure(symbol, policy)
    res.status(200).send("succeess!")
  }
}

exports.restoreDefaultSymbol = (req, res) => {
  let symbol = req.params.symbol
  procedureProcessor.setSymbolPocedure(symbol)
  res.status(200).send("succeess!")
}