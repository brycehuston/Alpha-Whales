import re

def fix():
    with open('src/execution.rs', 'r', encoding='utf-8') as f:
        code = f.read()

    # 1. attempt_buy_bundle: inject dynamic_amount_in
    code = code.replace(
        "        let mut get_quote_attempt = 1;\n        let quote = loop {\n            let quote_result = jito_client\n                .get_quote(\n                    target_mint,\n                    config.amount_in,",
        "        let dynamic_amount_in = (signal.trade_size_sol * 1_000_000_000.0) as u64;\n        let mut get_quote_attempt = 1;\n        let quote = loop {\n            let quote_result = jito_client\n                .get_quote(\n                    target_mint,\n                    dynamic_amount_in,"
    )
    
    code = code.replace(
        "            config.amount_in,\n            config.tip_lamports,\n            signed_bundle.transaction_fee_lamports,",
        "            dynamic_amount_in,\n            config.tip_lamports,\n            signed_bundle.transaction_fee_lamports,"
    )
    
    code = code.replace(
        "            amount_in: config.amount_in,\n            capital_at_risk_lamports,\n            timestamp_ms: signal.timestamp_ms,",
        "            amount_in: dynamic_amount_in,\n            capital_at_risk_lamports,\n            timestamp_ms: signal.timestamp_ms,"
    )

    # 2. construct_and_send_buy_bundle
    code = code.replace(
        "    config: &JitoExecutorConfig,\n) -> Result<PreparedSwap, JitoExecutionError> {\n    ensure_signal_fresh(signal, config.max_signal_age)?;\n    let target_mint = Pubkey::from_str(&signal.target_mint)",
        "    config: &JitoExecutorConfig,\n    dynamic_amount_in: u64,\n) -> Result<PreparedSwap, JitoExecutionError> {\n    ensure_signal_fresh(signal, config.max_signal_age)?;\n    let target_mint = Pubkey::from_str(&signal.target_mint)"
    )

    code = code.replace(
        "    let quote_future = fetch_raydium_quote(target_mint, config);",
        "    let quote_future = fetch_raydium_quote(target_mint, config, dynamic_amount_in);"
    )

    code = code.replace(
        "        config.amount_in,\n        true,\n    )?;\n\n    let user_destination_token_account =",
        "        dynamic_amount_in,\n        true,\n    )?;\n\n    let user_destination_token_account ="
    )

    code = code.replace(
        "    let minimum_amount_out = validate_quote(&quote, &pool_keys, signal, config)?;\n    let mut swap_instruction = construct_raydium_swap_instruction(\n        &pool_keys,\n        user_owner,\n        user_source_wsol_account,\n        user_destination_token_account,\n        config.amount_in,\n        minimum_amount_out,\n    )?;",
        "    let minimum_amount_out = validate_quote(&quote, &pool_keys, signal, config, dynamic_amount_in)?;\n    let mut swap_instruction = construct_raydium_swap_instruction(\n        &pool_keys,\n        user_owner,\n        user_source_wsol_account,\n        user_destination_token_account,\n        dynamic_amount_in,\n        minimum_amount_out,\n    )?;"
    )

    # 3. fetch_raydium_quote
    code = code.replace(
        "async fn fetch_raydium_quote(\n    target_mint: Pubkey,\n    config: &JitoExecutorConfig,\n) -> Result<QuoteData, JitoExecutionError> {",
        "async fn fetch_raydium_quote(\n    target_mint: Pubkey,\n    config: &JitoExecutorConfig,\n    dynamic_amount_in: u64,\n) -> Result<QuoteData, JitoExecutionError> {"
    )

    code = code.replace(
        "            (\"amount\", config.amount_in.to_string()),\n            (\"slippageBps\", config.max_slippage_bps.to_string()),",
        "            (\"amount\", dynamic_amount_in.to_string()),\n            (\"slippageBps\", config.max_slippage_bps.to_string()),"
    )

    # 4. validate_quote
    code = code.replace(
        "    signal: &WhaleSignal,\n    config: &JitoExecutorConfig,\n) -> Result<u64, JitoExecutionError> {",
        "    signal: &WhaleSignal,\n    config: &JitoExecutorConfig,\n    dynamic_amount_in: u64,\n) -> Result<u64, JitoExecutionError> {"
    )

    code = code.replace(
        "    if quoted_input != config.amount_in\n        || quoted_output == 0\n        || api_minimum_amount_out == 0\n        || local_minimum_amount_out == 0",
        "    if quoted_input != dynamic_amount_in\n        || quoted_output == 0\n        || api_minimum_amount_out == 0\n        || local_minimum_amount_out == 0"
    )

    # 5. process_swap_event
    code = code.replace(
        "        let prepared = match resolve_swap_instructions_for_signal(\n            &signal,\n            rpc_client.as_ref(),\n            &config,\n        )\n        .await\n        {",
        "        let dynamic_amount_in = (signal.trade_size_sol * 1_000_000_000.0) as u64;\n        let prepared = match resolve_swap_instructions_for_signal(\n            &signal,\n            rpc_client.as_ref(),\n            &config,\n            dynamic_amount_in,\n        )\n        .await\n        {"
    )

    code = code.replace(
        "        let pre_tip_profit = config.amount_in / 100;\n        let tip_decision =",
        "        let pre_tip_profit = dynamic_amount_in / 100;\n        let tip_decision ="
    )

    # 6. confirm_and_handoff
    code = code.replace(
        "    telegram_chat_id: Option<String>,\n) -> Result<(), JitoExecutionError> {\n    let signature = solana_sdk::signature::Signature::from_str(tx_signature_str)",
        "    telegram_chat_id: Option<String>,\n) -> Result<(), JitoExecutionError> {\n    let dynamic_amount_in = (signal.trade_size_sol * 1_000_000_000.0) as u64;\n    let signature = solana_sdk::signature::Signature::from_str(tx_signature_str)"
    )

    code = code.replace(
        "                            let trade_size = format!(\"Bot Trade: {} SOL\", (config.amount_in as f64) / 1_000_000_000.0);\n                            tokio::spawn(async move {",
        "                            let trade_size = format!(\"Bot Trade: {} SOL\", (dynamic_amount_in as f64) / 1_000_000_000.0);\n                            tokio::spawn(async move {"
    )

    code = code.replace(
        "    let position = ActivePosition {\n        mint: signal.target_mint.clone(),\n        source_pool_id: pool_id.clone(),\n        entry_price_wsol_num: config.amount_in as u128,\n        entry_price_wsol_den: acquired_amount as u128,",
        "    let position = ActivePosition {\n        mint: signal.target_mint.clone(),\n        source_pool_id: pool_id.clone(),\n        entry_price_wsol_num: dynamic_amount_in as u128,\n        entry_price_wsol_den: acquired_amount as u128,"
    )

    code = code.replace(
        "        tx_signature_str,\n        config.amount_in,\n        acquired_amount,",
        "        tx_signature_str,\n        dynamic_amount_in,\n        acquired_amount,"
    )

    # 7. resolve_swap_instructions_for_signal
    code = code.replace(
        "async fn resolve_swap_instructions_for_signal(\n    signal: &WhaleSignal,\n    rpc_client: &RpcClient,\n    config: &JitoExecutorConfig,\n) -> Result<PreparedSwap, JitoExecutionError> {",
        "async fn resolve_swap_instructions_for_signal(\n    signal: &WhaleSignal,\n    rpc_client: &RpcClient,\n    config: &JitoExecutorConfig,\n    dynamic_amount_in: u64,\n) -> Result<PreparedSwap, JitoExecutionError> {"
    )

    code = code.replace(
        "    let quote = jito_client.get_quote(\n        target_mint,\n        config.amount_in,\n        config.tip_lamports,\n    ).await.map_err(|e| JitoExecutionError::QuoteTimeout(e.to_string()))?;",
        "    let quote = jito_client.get_quote(\n        target_mint,\n        dynamic_amount_in,\n        config.tip_lamports,\n    ).await.map_err(|e| JitoExecutionError::QuoteTimeout(e.to_string()))?;"
    )

    code = code.replace(
        "    let bundle_result = construct_and_send_buy_bundle(\n        signal,\n        config.amount_in,\n        config.tip_lamports,\n        quote.input_mint,\n        quote.output_mint,\n        quote.out_amount,",
        "    let bundle_result = construct_and_send_buy_bundle(\n        signal,\n        dynamic_amount_in,\n        config.tip_lamports,\n        quote.input_mint,\n        quote.output_mint,\n        quote.out_amount,"
    )

    code = code.replace(
        "    let bundle_result = construct_and_send_buy_bundle(\n        signal,\n        config.amount_in,\n        config.tip_lamports,\n        quote.input_mint,\n        quote.output_mint,\n        quote.out_amount,",
        "    let bundle_result = construct_and_send_buy_bundle(\n        signal,\n        dynamic_amount_in,\n        config.tip_lamports,\n        quote.input_mint,\n        quote.output_mint,\n        quote.out_amount,"
    )

    with open('src/execution.rs', 'w', encoding='utf-8') as f:
        f.write(code)

if __name__ == '__main__':
    fix()
