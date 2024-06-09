const procedure = require("./orderMetaProcessors").statusAndExecutionProcessor

exports.procedures = {
    "fullAuto": [procedure.pendingToUnfilled, procedure.partiallyFilled, procedure.fullyFilled],
    "manuallyExecute": [procedure.pendingToUnfilled],
    "cancelled": [procedure.cancelled],
    "partiallyFilledCancelled": [procedure.pendingToUnfilled, procedure.partiallyFilled, procedure.cancelled]
}
exports.symbolProcedure = {}

let defaultProcedure = [procedure.pendingToUnfilled, procedure.partiallyFilled, procedure.fullyFilled]

exports.setDefaultPocedure = (policy) => {
    defaultProcedure = exports.procedures[policy]
}

exports.setSymbolPocedure = (symbol, policy='fullAuto') => {
    exports.symbolProcedure[symbol] = policy
}

exports.getProcedure = (order) => {
    if (exports.symbolProcedure.hasOwnProperty(order.symbol)) {
        return exports.procedures[exports.symbolProcedure[order.symbol]]
    } else {
        return defaultProcedure
    }
}