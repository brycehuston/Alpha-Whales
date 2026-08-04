const payload = {
    publicKey: '3pk5yRhwwFdvDBrBuuC5CRorVafHG2EQmzNEYy8Ny2K6',
    action: 'buy',
    mint: 'AWhYm15w6qpJ7hjAKjY3C3jbd6TThAckNpUsX7iwpump',
    amount: 0.05,
    denominatedInSol: 'true',
    slippage: 10,
    priorityFee: 0.0001,
    pool: 'pump'
};

fetch('https://pumpportal.fun/api/trade-local', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json'
    },
    body: JSON.stringify(payload)
})
.then(res => res.text().then(t => console.log(res.status, t)))
.catch(e => console.error(e));
