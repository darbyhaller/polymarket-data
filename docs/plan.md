## Pain Points
Mark any time periods where there haven't been any websocket updates for ANY market for 1 second as "PRICE UNKNOWN" rather than assuming constant price

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