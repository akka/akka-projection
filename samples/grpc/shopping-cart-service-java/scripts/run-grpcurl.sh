#!/bin/bash

cart=`date '+%Y%m%d-%H%M%S.%s'`-$RANDOM

grpcurl -d '{"cartId":"'$cart'", "itemId":"socks", "quantity":3}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.AddItem > /dev/null

for i in {1..5}
do
   grpcurl -d '{"cartId":"'$cart'", "itemId":"socks", "quantity":'$i'}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.UpdateItem > /dev/null
   sleep 0.02
done

grpcurl -d '{"cartId":"'$cart'"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.Checkout
