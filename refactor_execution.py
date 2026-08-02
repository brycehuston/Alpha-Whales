import sys
import re

def refactor():
    file_path = r"c:\FruxLabs\Alpha-Whales\src\execution.rs"
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
        
    # Replace ExecutionSignal with WhaleSignal
    content = content.replace("ExecutionSignal", "WhaleSignal")
    
    # Replace observed_at_ms with timestamp_ms
    content = content.replace("observed_at_ms", "timestamp_ms")
    
    # Remove quote_still_meets_trigger function definition
    content = re.sub(
        r"fn quote_still_meets_trigger.*?Ok\(current_side <= baseline_side\)\s*\}",
        "", 
        content,
        flags=re.DOTALL
    )
    
    # Remove quote_still_meets_trigger call
    content = re.sub(
        r"if \!quote_still_meets_trigger.*?return Err\(JitoExecutionError::QuoteNoLongerTriggers\);\s*\}",
        "",
        content,
        flags=re.DOTALL
    )
    
    # Remove source_pool_id parsing and checking
    content = re.sub(
        r"let source_pool_id = Pubkey::from_str\(&signal\.source_pool_id\).*?JitoExecutionError::InvalidSignalPool\(.*?\).*?\}\s*\}",
        "",
        content,
        flags=re.DOTALL
    )
    
    content = re.sub(
        r"if pool_keys\.amm_id \!\= source_pool_id\s*\{\s*return Err\(JitoExecutionError::SignalPoolMismatch\);\s*\}",
        "",
        content,
        flags=re.DOTALL
    )
    
    # Fix the opp_key source_pool_id to whale_wallet
    content = content.replace("pool: signal.source_pool_id.clone()", "pool: signal.whale_wallet.clone()")
    
    # Change function name
    content = content.replace("run_execution_consumer", "run_whale_execution_consumer")
    
    # Also change the channel type in the signature
    content = content.replace("mut signal_rx: UnboundedReceiver<WhaleSignal>", "mut signal_rx: tokio::sync::mpsc::Receiver<WhaleSignal>")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
        
    print("Execution refactored!")

if __name__ == "__main__":
    refactor()
