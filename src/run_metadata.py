import asyncio
import structlog
from services.metadata_generator import generate_metadata_schema
from extractors.generic import GenericExtractor
import llm 

logger = structlog.get_logger(__name__)

async def generate_wallet_metadata():
    # Test data
    actual_data = {
        "type": "gmgn_wallet_data",
        "url": "https://gmgn.ai/api/...",
        "data": {
            "rank": [
                {
                    "wallet_address": "abc123",
                    "realized_profit": 150.0,
                    "buy": 24,
                    "sell": 30,
                    "last_active": 1736817888,
                    "realized_profit_1d": 70879.49853688761,
                    "realized_profit_7d": 122662.43403346688,
                    "realized_profit_30d": 191269.16539506766,
                    "pnl_30d": 0.27870596416424176,
                    "pnl_7d": 2.2182305885237708,
                    "pnl_1d": 5.732970927245722,
                    "txs_30d": 396,
                    "buy_30d": 191,
                    "sell_30d": 205,
                    "balance": 761.744124299,
                    "eth_balance": 761.744124299,
                    "sol_balance": 761.744124299,
                    "trx_balance": 761.744124299,
                    "created_at": "2024-01-02T00:00:00Z",
                }
            ]
        },
    }

    # actual_data = {
    #                 "wallet_address": "abc123",
    #                 "realized_profit": 150.0,
    #                 "buy" : 24,
    #                 "sell" : 30,
    #                 "last_active" : 1736817888,
    #                 "realized_profit_1d" : 70879.49853688761,
    #                 "realized_profit_7d" : 122662.43403346688,
    #                 "realized_profit_30d" : 191269.16539506766,
    #                 "pnl_30d" : 0.27870596416424176,
    #                 "pnl_7d" : 2.2182305885237708,
    #                 "pnl_1d" : 5.732970927245722,
    #                 "txs_30d" : 396,
    #                 "buy_30d" : 191,
    #                 "sell_30d" : 205,
    #                 "balance" : 761.744124299,
    #                 "eth_balance" : 761.744124299,
    #                 "sol_balance" : 761.744124299,
    #                 "trx_balance" : 761.744124299,
    #                 "created_at": "2024-01-02T00:00:00Z",
    #             }

 
    # Extract sample item and generate schema
    sample_item, _ = GenericExtractor.extract_sample_item(actual_data, "wallet_address")
    logger.debug(f"Sample Item: {sample_item}")
    
    schema = await generate_metadata_schema(sample_item, llm)
    logger.debug(f"Schema: {schema}")
    return schema

if __name__ == "__main__":
    # Run the test
    schema = asyncio.run(generate_wallet_metadata())
    print("Generated Schema:")
    for metric in schema:
        print(f"- {metric.name}: {metric.description}") 
