import base58, ssl, json, urllib.request

KEY = "3XcVBeGuiCw6VSpczAVc6ja5VDPtGUMArXhXjZziJrHEZ17J1qTFXxMkbWTs54PV6W7b4R7KjpMVFWyPCbaMuWGF"
key_bytes = base58.b58decode(KEY)
pubkey_bytes = key_bytes[32:]
pubkey = base58.b58encode(pubkey_bytes).decode()
print(f"Wallet pubkey: {pubkey}")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
RPC = "https://mainnet.helius-rpc.com/?api-key=dc712866-82f4-4987-940c-07eab65e427a"

def rpc(method, params):
    req = urllib.request.Request(
        RPC,
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode(),
        headers={"Content-Type":"application/json"}
    )
    return json.loads(urllib.request.urlopen(req, context=ctx, timeout=10).read())

sol = rpc("getBalance", [pubkey])
sol_bal = sol["result"]["value"] / 1e9
print(f"SOL balance: {sol_bal:.6f} SOL")

wsol = rpc("getTokenAccountsByOwner", [
    pubkey,
    {"mint": "So11111111111111111111111111111111111111112"},
    {"encoding": "jsonParsed"}
])
accs = wsol["result"]["value"]
if accs:
    for a in accs:
        ui = a["account"]["data"]["parsed"]["info"]["tokenAmount"]["uiAmount"]
        print(f"WSOL balance: {ui} WSOL")
else:
    print("WSOL: NO ACCOUNT")

print(f"\n--- VERDICT ---")
if sol_bal < 0.05:
    print("WALLET IS EMPTY OR NEAR-EMPTY - top up before bot can trade")
elif sol_bal < 0.25:
    print(f"Trades possible: ~{int(sol_bal / 0.053)} at 0.05 SOL each - OK for testing")
else:
    print(f"Trades possible: ~{int(sol_bal / 0.053)} at 0.05 SOL each - Good")
