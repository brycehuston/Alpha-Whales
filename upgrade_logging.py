import sys

def replace_logging():
    file_path = r"c:\FruxLabs\Alpha-Whales\src\execution.rs"
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
        
    content = content.replace("eprintln!(", "log::warn!(")
    
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
        
    print("Logging upgraded!")

if __name__ == "__main__":
    replace_logging()
