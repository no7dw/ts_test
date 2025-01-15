import json
from datetime import datetime
import structlog

logger = structlog.get_logger(__name__)

def convert_iso_to_timestamp(iso_date_str):
    """Convert ISO date string to Unix timestamp"""
    try:
        dt = datetime.fromisoformat(iso_date_str.replace('Z', '+00:00'))
        return int(dt.timestamp())
    except Exception as e:
        logger.error(f"Failed to convert date: {str(e)}")
        return None

def extract_wallet_data(input_file, output_file):
    """Extract wallet data from gmgn.json and write to sol_smart_wallet.json"""
    try:
        # Read input file
        with open(input_file, 'r') as f:
            data_list = json.load(f)

        # Extract relevant data
        extracted_data = []
        
        # Process each document in the data list
        for doc in data_list:
            timestamp = convert_iso_to_timestamp(doc.get('created_at'))
            if not timestamp:
                continue

            # Get the rank data from the document
            rank_data = doc.get('data', {}).get('rank', [])
            
            # Process each wallet in the rank data
            for wallet_data in rank_data:
                wallet_address = wallet_data.get('wallet_address')
                if not wallet_address:
                    continue

                # Extract realized profit
                realized_profit = wallet_data.get('realized_profit')
                if realized_profit is not None:
                    extracted_data.append({
                        "entity": wallet_address,
                        "metric": "Realized Profit",
                        "value": realized_profit,
                        "timestamp": timestamp,
                        "source": "https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d?tag=snipe_bot&orderby=pnl_7d&direction=desc"
                    })

                # Extract PnL
                PnL = wallet_data.get('pnl_7d')
                if PnL is not None:
                    extracted_data.append({
                        "entity": wallet_address,
                        "metric": "PnL",
                        "value": PnL,
                        "timestamp": timestamp,
                        "source": "https://gmgn.ai/defi/quotation/v1/rank/sol/wallets/7d?tag=snipe_bot&orderby=pnl_7d&direction=desc"
                    })

        # Write to output file
        with open(output_file, 'w') as f:
            json.dump(extracted_data, f, indent=2)
            
        logger.info(f"Successfully extracted {len(extracted_data)} records")
        
    except Exception as e:
        logger.error(f"Failed to process data: {str(e)}")
        raise

if __name__ == "__main__":
    input_file = "gmgn.json"
    output_file = "sol_smart_wallet.json"
    extract_wallet_data(input_file, output_file) 
