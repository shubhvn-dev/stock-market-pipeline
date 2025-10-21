import json
import threading
from kafka import KafkaConsumer
from collections import deque
import logging

logger = logging.getLogger(__name__)

class KafkaDataProvider:
    def __init__(self):
        self.llm_analyses = deque(maxlen=50)
        self.alerts = deque(maxlen=100)
        self.latest_indicators = {}
        self.running = False
        
    def start_consumers(self):
        """Start Kafka consumers in background threads"""
        self.running = True
        
        # Start LLM analysis consumer
        llm_thread = threading.Thread(target=self._consume_llm_analysis)
        llm_thread.daemon = True
        llm_thread.start()
        
        # Start alerts consumer
        alerts_thread = threading.Thread(target=self._consume_alerts)
        alerts_thread.daemon = True
        alerts_thread.start()
        
        logger.info("Kafka consumers started")
    
    def _consume_llm_analysis(self):
        """Consume LLM analysis from Kafka"""
        try:
            consumer = KafkaConsumer(
                'stock-analysis',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            
            for message in consumer:
                if not self.running:
                    break
                    
                analysis_data = message.value
                self.llm_analyses.append(analysis_data)
                
        except Exception as e:
            logger.error(f"LLM consumer error: {e}")
    
    def _consume_alerts(self):
        """Consume alerts from Kafka"""
        try:
            consumer = KafkaConsumer(
                'stock-alerts',
                bootstrap_servers=['localhost:9092'],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            
            for message in consumer:
                if not self.running:
                    break
                    
                alert_data = message.value
                self.alerts.append(alert_data)
                
        except Exception as e:
            logger.error(f"Alerts consumer error: {e}")
    
    def get_latest_llm_analysis(self, symbol):
        """Get latest LLM analysis for a symbol"""
        symbol_analyses = [a for a in self.llm_analyses if a['symbol'] == symbol]
        return symbol_analyses[-1] if symbol_analyses else None
    
    def get_recent_alerts(self, limit=5):
        """Get recent alerts"""
        return list(self.alerts)[-limit:] if self.alerts else []
    
    def stop(self):
        """Stop consumers"""
        self.running = False
