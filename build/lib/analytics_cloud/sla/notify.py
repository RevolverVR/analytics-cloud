import os

def main():
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    # Placeholder: luego mandaremos Slack/Email/Telegram de verdad
    print(f"[sla] notify OK (placeholder). Slack configured? {'yes' if webhook else 'no'}")

if __name__ == "__main__":
    main()
