const portfolioData = {}
portfolioData[80038311000] = {
    'cash': {
        "USD": 1_000_000,
        "HKD": 1_000_000,
        "CNH": 1_000_000,
        "CNY": 1_000_000
    },
    'holdings': [{
        'symbol': 'AGG',
        'averagePrice': 116.90,
        'quantity': 100
    }, {
        'symbol': 'JPST',
        'averagePrice': 50.60,
        'quantity': 233
    }]
}

portfolioData[80038311001] = {
    'cash': {
        "USD": 1_000_000,
        "HKD": 1_000_000,
        "CNH": 1_000_000,
        "CNY": 1_000_000
    },
    'holdings': []
  }
  portfolioData[80038311002] = {
    'cash': {
        "USD": 1_000_000,
        "HKD": 1_000_000,
        "CNH": 1_000_000,
        "CNY": 1_000_000
    },
    'holdings': []
  }
  portfolioData[80038311003] = {
    'cash': {
        "USD": 500_000,
        "HKD": 500_000,
        "CNH": 500_000,
        "CNY": 500_000
    },
    'holdings': []
  }
  
let baseBrokerAccount = 80038312000
for (let i = 0; i < 100; i++) {
    let brokerAccount = baseBrokerAccount+i
    portfolioData[brokerAccount] = {
        'cash': {
        "USD": 1_000_000,
        "HKD": 1_000_000,
        "CNH": 1_000_000,
        "CNY": 1_000_000
    },
        'holdings': []
    }
}

baseBrokerAccount = 80038313000
for (let i = 0; i < 100; i++) {
    let brokerAccount = baseBrokerAccount+i
    portfolioData[brokerAccount] = {
        'cash': {
            "USD": 10_000_000,
            "HKD": 10_000_000,
            "CNH": 10_000_000,
            "CNY": 10_000_000
        },
        'holdings': []
    }
}
exports.portfolioData = portfolioData;