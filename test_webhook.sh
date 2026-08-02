#!/bin/bash
curl -X POST http://localhost:5000/webhook \
  -H "Content-Type: application/json" \
  -H "Authorization: supersecret" \
  -d '[{
    "type": "SWAP",
    "feePayer": "HZCAVtP3crMNkuporiWaY6HY7r5bsTFTcQfXcgfjyjt9",
    "events": {
      "swap": {
        "nativeInput": {
          "amount": "500000000"
        },
        "tokenOutputs": [
          {
            "mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
          }
        ]
      }
    }
  }]'
