use reqwest::Client;
use serde_json::json;

#[tokio::main]
async fn main() {
    let http_client = Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();
    let bot_token = "7718954879:AAEv34T0_y7mJYV4WNW-QfSGqqK6gv9XgDw";
    let chat_id = "823456450";
    let pubkey = "TestWalletKey";
    
    let time_str = "2026-08-01 12:00:00 UTC";

    let message = format!(
        "<b>🟢 ALPHA NEXUS ONLINE 🟢</b>\n\
        ━━━━━━━━━━━━━━━━━━━━━━━━\n\
        <b>Bot State:</b> Active & Listening\n\
        <b>Wallet:</b> <code>{}</code>\n\
        <b>Time:</b> {}\n\
        ━━━━━━━━━━━━━━━━━━━━━━━━\n\
        ʜᴛꜰ © ᴀʟᴘʜᴀ ᴀʟᴇʀᴛꜱ | v1.01",
        pubkey, time_str
    );

    let url = format!("https://api.telegram.org/bot{}/sendMessage", bot_token);
    let payload = json!({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": true,
    });

    match http_client
        .post(&url)
        .json(&payload)
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await {
            Ok(res) => {
                let status = res.status();
                let text = res.text().await.unwrap();
                println!("Status: {}\nBody: {}", status, text);
            }
            Err(e) => {
                println!("Error: {}", e);
            }
        }
}
