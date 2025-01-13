# ts-test


# example output 

```
rrp appm.py  "What is the total value locked on arbitrum?"
2025-01-09 17:03:10,319 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
```json
{
    "entity": "TVL",
    "filter": {
        "chain": "Arbitrum"
    }
}
```
2025-01-09 17:03:14,432 - INFO - HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
To determine the Total Value Locked (TVL) on Arbitrum, we can look at the provided data from trusted sources. Two different sources give us different values for Arbitrum's TVL:

1. From DefiLlama: 

   - Value: $2,999,000,000 (January 8, 2025)

2. From Footprint Network:

   - Value: $9,681,676,288.17 (January 7, 2025)

Since these values come from different sources and are dated closely together, it's essential to consider the most recent data. The value from DefiLlama is more recent (January 8, 2025), which makes it a stronger candidate for the current TVL.

Therefore, the Total Value Locked on Arbitrum as of the latest data is approximately **$2.999 billion**. 

However, it's important to note that TVL can fluctuate, and different sources might provide varying numbers based on their calculations and updates. For the most accurate result, it's advisable to check the latest updates directly from these sources.
```
