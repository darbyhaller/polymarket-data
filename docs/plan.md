## Pain Points
last_trade_price STILL NOT WRITING on hour
order_update still .5% corrupt. invalid footer. why?
- It's always the last one in the hour

## Future Directions
* Ground with search
* Reinforcement learning on trades
* Use e^(sentence bert sim of titles) as prior edge weight between markets
* graph in logits

## Stats
* Tokens/s: 10K

## Data Streams
* wss://ws-subscriptions-clob.polymarket.com/ws/market contains:
    * filled limit orders (trades)
    * order book changes