import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.alerts.email_notifier import EmailNotifier

def main():
    notifier = EmailNotifier()
    notifier.process_alerts()

if __name__ == "__main__":
    main()
