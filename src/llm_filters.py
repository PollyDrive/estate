import json
import logging
import os
from typing import Dict, Optional, Tuple
from groq import Groq
from anthropic import Anthropic

logger = logging.getLogger(__name__)


class Level1Filter:
    """
    Level 1 filter: Free LLM filter using Groq (Llama 3).
    Confirms presence of kitchen suitable for cooking.
    """
    
    def __init__(self, config: Dict, api_key: str):
        """
        Initialize Level 1 filter with Groq API.
        
        Args:
            config: Configuration dictionary
            api_key: Groq API key
        """
        self.config = config['llm']['groq']
        self.client = Groq(api_key=api_key)
    
    def filter(self, description: str) -> Tuple[bool, str]:
        """
        Check if listing has a kitchen using Groq LLM.
        
        Args:
            description: Listing description
            
        Returns:
            Tuple of (passed: bool, reason: str)
        """
        try:
            prompt = self.config['prompt_template'].format(description=description)
            
            response = self.client.chat.completions.create(
                model=self.config['model'],
                messages=[{"role": "user", "content": prompt}],
                temperature=self.config['temperature'],
                max_tokens=self.config['max_tokens']
            )
            
            answer = response.choices[0].message.content.strip().upper()
            
            if 'YES' in answer:
                logger.info("Level 1 filter: Kitchen confirmed by Groq")
                return True, "Kitchen confirmed"
            else:
                logger.info("Level 1 filter: No kitchen found by Groq")
                return False, "No kitchen confirmed"
                
        except Exception as e:
            logger.error(f"Groq API error: {e}")
            # In case of error, pass to next level to avoid false negatives
            return True, f"Groq error (passed): {str(e)}"


class Level2Filter:
    """
    Level 2 filter: Paid LLM analysis using Claude 3 Haiku.
    Generates summary and contact messages in multiple languages.
    """
    
    def __init__(self, config: Dict, api_key: str):
        """
        Initialize Level 2 filter with Anthropic API.
        
        Args:
            config: Configuration dictionary
            api_key: Anthropic API key
        """
        self.config = config['llm']['claude']
        self.client = Anthropic(api_key=api_key)
    
    def filter(
        self,
        title: str,
        price: str,
        description: str
    ) -> Tuple[bool, Optional[Dict], str]:
        """
        Analyze listing and generate summary and messages.
        
        Args:
            title: Listing title
            price: Listing price
            description: Listing description
            
        Returns:
            Tuple of (passed: bool, response_data: Optional[Dict], reason: str)
            response_data contains: summary_ru, msg_en, msg_id
        """
        try:
            prompt = self.config['prompt_template'].format(
                criteria=self.config['search_criteria'],
                title=title,
                price=price,
                description=description
            )
            
            message = self.client.messages.create(
                model=self.config['model'],
                max_tokens=self.config['max_tokens'],
                temperature=self.config['temperature'],
                messages=[{"role": "user", "content": prompt}]
            )
            
            response_text = message.content[0].text
            
            # Try to parse JSON from response
            try:
                # Extract JSON from response (Claude might add extra text)
                json_start = response_text.find('{')
                json_end = response_text.rfind('}') + 1
                
                if json_start >= 0 and json_end > json_start:
                    json_str = response_text[json_start:json_end]
                    response_data = json.loads(json_str)
                    
                    # Validate required fields
                    required_fields = ['summary_ru', 'msg_en', 'msg_id']
                    if all(field in response_data for field in required_fields):
                        logger.info("Level 2 filter: Analysis completed by Claude")
                        return True, response_data, "Analysis completed"
                    else:
                        logger.error(f"Missing required fields in Claude response: {response_data}")
                        return False, None, "Invalid response format"
                else:
                    logger.error(f"No JSON found in Claude response: {response_text}")
                    return False, None, "No JSON in response"
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse Claude JSON response: {e}")
                logger.error(f"Response text: {response_text}")
                return False, None, "JSON parse error"
                
        except Exception as e:
            logger.error(f"Claude API error: {e}")
            return False, None, f"Claude error: {str(e)}"
