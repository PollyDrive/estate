import logging
import requests
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class TelegramNotifier:
    """Telegram notification manager."""
    
    def __init__(self, bot_token: str, chat_id: str, config: Dict):
        """
        Initialize Telegram notifier.
        
        Args:
            bot_token: Telegram bot token
            chat_id: Telegram chat ID
            config: Configuration dictionary
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.message_template = config['telegram']['message_template']
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    def send_message(self, message: str) -> bool:
        """
        Send a pre-formatted message to Telegram.
        
        Args:
            message: Pre-formatted message text
            
        Returns:
            True if sent successfully, False otherwise
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
                logger.info(f"Telegram message sent successfully")
                return True
            else:
                logger.error(f"Telegram API error: {response.status_code} - {response.text}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending Telegram message: {e}")
            return False
    
    def send_notification(
        self,
        summary_ru: str,
        price: str,
        phone: Optional[str],
        url: str
    ) -> bool:
        """
        Send notification to Telegram group.
        
        Args:
            summary_ru: Russian summary from Claude
            price: Listing price
            phone: Phone number (if found)
            url: Listing URL
            
        Returns:
            True if sent successfully, False otherwise
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
                logger.info(f"Telegram notification sent successfully")
                return True
            else:
                logger.error(f"Telegram API error: {response.status_code} - {response.text}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Failed to send Telegram notification: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending Telegram notification: {e}")
            return False
