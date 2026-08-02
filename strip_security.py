import sys

def strip_security_from_main():
    file_path = r"c:\FruxLabs\Alpha-Whales\src\main.rs"
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
        
    # Remove security config and coordinator
    content = content.replace("let security_config = security::SecurityConfig::from_env()?;", "")
    content = content.replace("let security_coordinator =\n        security::SecurityCoordinator::new(rpc_client.clone(), security_config);", "")
    content = content.replace("let near_miss_coordinator = security_coordinator.clone();", "")
    
    # Remove from webhook state
    content = content.replace("security_coordinator: security_coordinator.clone(),", "")
    
    # Remove from execution clone
    content = content.replace("let security_clone = security_coordinator.clone();", "")
    
    # Remove from execution call
    content = content.replace("security_clone,", "")
    
    # Remove from heartbeat metrics
    content = content.replace("let health_metrics = security_coordinator.metrics.clone();", "")
    content = content.replace("let ss = health_metrics.scan_started_total.load(Ordering::Relaxed);", "")
    content = content.replace("let sp = health_metrics.scan_passed_total.load(Ordering::Relaxed);", "")
    content = content.replace("let sf = health_metrics.scan_failed_total.load(Ordering::Relaxed);", "")
    content = content.replace("let se = health_metrics.scan_error_total.load(Ordering::Relaxed);", "")
    content = content.replace("let st = health_metrics.scan_timeout_total.load(Ordering::Relaxed);", "")
    content = content.replace("let dur_sum = health_metrics.scan_duration_ms_sum.load(Ordering::Relaxed);", "")
    content = content.replace("let dur_mean_ms = dur_sum.checked_div(ss).unwrap_or(0);", "")
    
    content = content.replace("scan_started={ss} \\", "")
    content = content.replace("scan_passed={sp} \\", "")
    content = content.replace("scan_failed={sf} \\", "")
    content = content.replace("scan_error={se} \\", "")
    content = content.replace("scan_timeout={st} \\", "")
    content = content.replace("scan_dur_ms={dur_mean_ms} \\", "")
    
    content = content.replace("mod security;", "")
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
        
    print("Stripped security from main.rs")

if __name__ == "__main__":
    strip_security_from_main()
