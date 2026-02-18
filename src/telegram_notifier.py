import logging
import os
import requests
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Telegram notification manager."""
    
    def __init__(self, bot_token: str, chat_id: str, config: Dict, admin_chat_id: Optional[str] = None):
        """
        Initialize Telegram notifier.
        
        Args:
            bot_token: Telegram bot token
            chat_id: Telegram chat ID
            config: Configuration dictionary
            admin_chat_id: Optional Telegram ID for administrative alerts
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.admin_chat_id = admin_chat_id or os.getenv('TELEGRAM_ADMIN_ID')
        self.message_template = config['telegram']['message_template']
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    def send_admin_message(self, text: str) -> Optional[int]:
        """Send a message to the admin chat ID."""
        target_id = self.admin_chat_id or self.chat_id
        try:
            payload = {
                'chat_id': target_id,
                'text': text,
                'parse_mode': 'Markdown'
            }
            response = requests.post(self.api_url, json=payload, timeout=10)
            if response.status_code == 200:
                return response.json()['result']['message_id']
            logger.error(f"Admin Telegram API error: {response.status_code} - {response.text}")
            return None
        except Exception as e:
            logger.error(f"Failed to send admin Telegram message: {e}")
            return None

    def send_error(self, stage: str, error_msg: str) -> Optional[int]:
        """Format and send an error alert to admin."""
        import socket
        hostname = socket.gethostname()
        text = f"❌ *Pipeline Error*\n\n*Stage:* {stage}\n*Host:* `{hostname}`\n\n*Error:* \n`{error_msg}`"
        return self.send_admin_message(text)
    
    def send_message(self, message: str) -> Optional[int]:
        """
        Send a pre-formatted message to Telegram.

        Args:
            message: Pre-formatted message text

        Returns:
            Telegram message_id if sent successfully, None otherwise
        """
        try:
            # Send message via Telegram API
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }

            response = requests.post(self.api_url, json=payload, timeout=10)

            if response.status_code == 200:
                data = response.json()
                message_id = data['result']['message_id']
                logger.info(f"Telegram message sent successfully (message_id: {message_id})")
                return message_id
            else:
                logger.error(f"Telegram API error: {response.status_code} - {response.text}")
                return None

        except requests.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error sending Telegram message: {e}")
            return None
    
    def send_notification(
        self,
        summary_ru: str,
        price: str,
        phone: Optional[str],
        url: str
    ) -> Optional[int]:
        """
        Send notification to Telegram group.

        Args:
            summary_ru: Russian summary from Claude
            price: Listing price
            phone: Phone number (if found)
            url: Listing URL

        Returns:
            Telegram message_id if sent successfully, None otherwise
        """
        try:
            # Format phone number display
            phone_display = phone if phone else "Не найден"

            # Format message using template
            message = self.message_template.format(
                summary_ru=summary_ru,
                price=price,
                phone=phone_display,
                url=url
            )

            # Send message via Telegram API
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            }

            response = requests.post(self.api_url, json=payload, timeout=10)

            if response.status_code == 200:
                data = response.json()
                message_id = data['result']['message_id']
                logger.info(f"Telegram notification sent successfully (message_id: {message_id})")
                return message_id
            else:
                logger.error(f"Telegram API error: {response.status_code} - {response.text}")
                return None

        except requests.RequestException as e:
            logger.error(f"Failed to send Telegram notification: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error sending Telegram notification: {e}")
            return None
