import sys

def refactor():
    file_path = r"c:\FruxLabs\Alpha-Whales\src\execution.rs"
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        
    for i in range(len(lines)):
        lines[i] = lines[i].replace("ExecutionSignal", "WhaleSignal")
        lines[i] = lines[i].replace("observed_at_ms", "timestamp_ms")
        
        # In resolve_swap_instructions_for_signal, remove source_pool_id 
        if "let source_pool_id = Pubkey::from_str(&signal.source_pool_id)" in lines[i]:
            lines[i] = ""
            lines[i+1] = ""
            lines[i+2] = ""
        if "if source_pool_id == Pubkey::default() {" in lines[i]:
            lines[i] = ""
            lines[i+1] = ""
            lines[i+2] = ""
            lines[i+3] = ""
        if "if pool_keys.amm_id != source_pool_id {" in lines[i]:
            lines[i] = ""
            lines[i+1] = ""
            lines[i+2] = ""
            
        # Remove quote_still_meets_trigger inside validate_quote
        if "if !quote_still_meets_trigger(config.amount_in, minimum_amount_out, signal.vwap_baseline)?" in lines[i]:
            lines[i] = ""
            lines[i+1] = ""
            lines[i+2] = ""
            
        # In opp_key
        lines[i] = lines[i].replace("pool: signal.source_pool_id.clone()", "pool: signal.whale_wallet.clone()")
        
        # run_execution_consumer
        lines[i] = lines[i].replace("run_execution_consumer", "run_whale_execution_consumer")
        lines[i] = lines[i].replace("mut signal_rx: UnboundedReceiver<WhaleSignal>", "mut signal_rx: tokio::sync::mpsc::Receiver<WhaleSignal>")

    with open(file_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
        
    print("Execution safely refactored!")

if __name__ == "__main__":
    refactor()
